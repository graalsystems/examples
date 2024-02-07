import cv2
import time
import torch
import torch.nn as nn
from torchvision import transforms
from PIL import Image
from torchvision import models
from smart_open import open as smart_open
import io

import os
from time import sleep
import grpc
import camera_pb2
import camera_pb2_grpc
import threading
import logging
import queue
import traceback


class CameraFeed:
    def __init__(self, url):
        global global_stop_event

        self.url = url
        self.queue = queue.Queue(1)
        self.thread = None
        self.stop_event = global_stop_event

    def start_handler(self):
        self.thread = threading.Thread(target=self.get_frames)
        self.thread.start()

    def wait_handler(self):
        if self.thread is not None:
            self.thread.join()

    # Generator function for video streaming.
    def generator_func(self):
        while not self.stop_event.wait(0.01):
            frame = self.queue.get(True, None)
            yield frame

    # Loops, creating gRPC client and grabbing frame from camera serving specified url.
    def get_frames(self):
        logging.info("Starting get_frames(%s)" % self.url)
        while not self.stop_event.wait(0.01):
            try:
                client_channel = grpc.insecure_channel(self.url, options=(
                    ('grpc.use_local_subchannel_pool', 1),))
                camera_stub = camera_pb2_grpc.CameraStub(client_channel)
                frame = camera_stub.GetFrame(camera_pb2.NotifyRequest())
                frame = frame.frame
                client_channel.close()

                frame_received = False
                # prevent stale data
                if len(frame) > 0:
                    if self.queue.full():
                        try:
                            self.queue.get(False)
                        except:
                            pass
                    self.queue.put(frame, False)
                    frame_received = True

                if frame_received:
                    sleep(1)

            except:
                logging.info("[%s] Exception %s" % (self.url, traceback.format_exc()))
                sleep(1)


class CameraDisplay:
    def __init__(self):
        self.main_camera = None
        self.small_cameras = []
        self.mutex = threading.Lock()

    def start_handlers(self):
        if self.main_camera is not None:
            self.main_camera.start_handler()

    def wait_handlers(self):
        global global_stop_event

        global_stop_event.set()
        if self.main_camera is not None:
            self.main_camera.wait_handler()
        global_stop_event.clear()

    def merge(self, other):
        self.mutex.acquire()
        try:
            self.wait_handlers()

            self.main_camera = other.main_camera

            self.start_handlers()
        finally:
            self.mutex.release()

    def count(self):
        self.mutex.acquire()
        result = 0
        if self.main_camera is not None:
            result += 1
        self.mutex.release()
        return result

    def hash_code(self):
        self.mutex.acquire()
        cameras = ""
        if self.main_camera is not None:
            cameras = f"{self.main_camera.url}"
        self.mutex.release()
        return cameras

    def stream_frames(self, camera_id=0):
        selected_camera = None
        self.mutex.acquire()
        if camera_id == 0:
            selected_camera = self.main_camera
        self.mutex.release()

        return selected_camera.generator_func()


def get_camera_display():
    camera_display_obj = CameraDisplay()
    url_webcam = os.getenv("VIDEO_BROKER_IP") + ":80"
    camera_display_obj.main_camera = CameraFeed(url_webcam)
    return camera_display_obj


def refresh_cameras():
    global global_camera_display
    while True:
        sleep(1)
        camera_display = get_camera_display()
        if camera_display != global_camera_display:
            global_camera_display.merge(camera_display)


def load_fine_tuned_model(saved_model_path):
    model = models.resnet18(pretrained=True)
    num_classes = 1
    model.fc = nn.Linear(model.fc.in_features, num_classes)
    with smart_open(saved_model_path, 'rb') as f:
        buffer = io.BytesIO(f.read())
        model.load_state_dict(torch.load(buffer, map_location=torch.device('cpu')))
    model.eval()
    return model


def classify_image(image_path, model):
    # Preprocess the image
    transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
    ])
    image = Image.open(image_path).convert("RGB")
    input_img = transform(image).unsqueeze(0)

    # Make prediction
    with torch.no_grad():
        outputs = torch.sigmoid(model(input_img))
        results = (outputs > 0.5).float().view(-1)

    return results.item()


def capture_and_classify_images(img_folder, model, camera_stream):
    # Create the output folder if it doesn't exist
    if not os.path.exists(img_folder):
        os.makedirs(img_folder)

    try:
        while True:
            # Capture a frame from the webcam
            webcam_frame = camera_stream.stream_frames(camera_id=0)
            print(f"Type image webcam: {type(webcam_frame)}")

            # Generate a unique filename based on the current timestamp
            timestamp = time.strftime("%Y%m%d%H%M%S")
            filename = f"{img_folder}/image_{timestamp}.jpg"
            print(f"Capturing {filename}")

            # Save the captured frame to the designated folder
            cv2.imwrite(filename, webcam_frame)
            # print(f"Image saved: {filename}")

            # Classify the captured image
            predicted_class = classify_image(filename, model)
            # print(f"Predicted class: {predicted_class}")

            # Output a message based on the prediction
            if predicted_class == 0:
                print("You can do better...")
            else:
                print("Nice LittleBigCode mug !")

            # Wait for 1 second before capturing the next image
            os.remove(filename)

    except KeyboardInterrupt:
        # Handle Ctrl+C to gracefully exit the loop
        print("Capture stopped by user")


if __name__ == "__main__":
    global_stop_event = threading.Event()
    global_camera_display = CameraDisplay()
    camera_display = get_camera_display()
    global_camera_display.merge(camera_display)

    refresh_thread = threading.Thread(target=refresh_cameras)
    refresh_thread.start()

    output_folder = "images"  # Specify the folder where images will be saved
    model_path = "https://graal-demo-data-integration.s3.fr-par.scw.cloud/cannes/fine_tuned_model.pth?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Credential=SCW2XE8GNS20FZ2EWDD9%2F20240207%2Ffr-par%2Fs3%2Faws4_request&X-Amz-Date=20240207T094844Z&X-Amz-Expires=33077&X-Amz-Signature=d4e97f4e7d8b3915bd540e5908b3f160f9954620def6f913d446b771d00e5dab&X-Amz-SignedHeaders=host&x-id=GetObject"
    fine_tuned_model = load_fine_tuned_model(model_path)
    capture_and_classify_images(output_folder, fine_tuned_model, global_camera_display)
