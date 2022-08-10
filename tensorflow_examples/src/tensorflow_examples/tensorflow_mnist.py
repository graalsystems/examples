import tensorflow_datasets as tfds
import tensorflow as tf
from tensorflow.keras import layers, models


# Import and preprocess the data
def make_datasets_unbatched():
    BUFFER_SIZE = 10000

    def scale(image, label):
        image = tf.cast(image, tf.float32)
        image /= 255
        return image, label

    datasets, _ = tfds.load(name='mnist', with_info=True, as_supervised=True)

    return datasets['train'].map(scale).cache().shuffle(BUFFER_SIZE)

# Create a model for classification purposes
def build_and_compile_cnn_model():
    model = models.Sequential()
    model.add(
        layers.Conv2D(32, (3, 3), activation='relu', input_shape=(28, 28, 1)))
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Conv2D(64, (3, 3), activation='relu'))
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Conv2D(64, (3, 3), activation='relu'))
    model.add(layers.Flatten())
    model.add(layers.Dense(64, activation='relu'))
    model.add(layers.Dense(10, activation='softmax'))

    model.summary()

    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])

    return model


def main():

    BATCH_SIZE = 64
    ds_train = make_datasets_unbatched().batch(BATCH_SIZE).repeat()
    model = build_and_compile_cnn_model()
    model.fit(ds_train,
              epochs=10,
              steps_per_epoch=100)


if __name__ == '__main__':
    main()