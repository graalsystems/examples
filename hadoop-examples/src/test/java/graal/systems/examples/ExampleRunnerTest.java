package graal.systems.examples;

import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.net.io.CopyStreamAdapter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class ExampleRunnerTest {

    private final static Logger LOG = LoggerFactory.getLogger(ExampleRunnerTest.class);

    @Test
    public void test() throws IOException {
        ExampleRunner.main("s3://datalake/input.csv", "s3://datalake/res2.csv");
    }

//    @Test
    public void an() throws IOException {
//        final JobConf test = new JobConf(MRJobConfig.JOB_CONF_FILE);
//        test.writeXml(System.out);

        byte[] shufflePassword = "shuffle password".getBytes(StandardCharsets.UTF_8);
        byte[] jobPassword = "job password".getBytes(StandardCharsets.UTF_8);
        byte[] jobId = "job_xxddddddddddx".getBytes(StandardCharsets.UTF_8);

        Text jobToken = new Text("JobToken");
//        -------- old
        Credentials credentials1 = new Credentials();
        Token<TokenIdentifier> rawToken = new Token<>(
                jobId,
                jobPassword,
                new Text("mapreduce.job"), null);
        credentials1.addToken(jobToken, rawToken);
        System.out.println("old - 1");
        credentials1.write(new DataOutputStream(new Base64OutputStream(System.out)));
        System.out.println("old - 2");
        credentials1.write(new DataOutputStream(System.out));

        ByteArrayOutputStream outOld = new ByteArrayOutputStream();
        credentials1.write(new DataOutputStream(outOld));
        JobTokenIdentifier jobTokenIdentifier = new JobTokenIdentifier();
        jobTokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(outOld.toByteArray())));
        System.out.println("old - 3");
        System.out.println(jobTokenIdentifier.getJobId());

//        -------- new
        Credentials credentials2 = new Credentials();
        SecretManager<JobTokenIdentifier> secretManager = new SecretManager<JobTokenIdentifier>() {
            @Override
            protected byte[] createPassword(JobTokenIdentifier jobTokenIdentifier) {
                return jobPassword;
            }

            @Override
            public byte[] retrievePassword(JobTokenIdentifier jobTokenIdentifier) throws InvalidToken {
                return jobPassword;
            }

            @Override
            public JobTokenIdentifier createIdentifier() {
                return null;
            }
        };
        Token<JobTokenIdentifier> token = new Token<>(new JobTokenIdentifier(),
                secretManager);
        credentials2.addToken(jobToken, token);
        System.out.println("new - 1");
        credentials2.write(new DataOutputStream(new Base64OutputStream(System.out)));
        System.out.println("new - 2");
        credentials2.write(new DataOutputStream(System.out));

        ByteArrayOutputStream outNew = new ByteArrayOutputStream();
        credentials2.write(new DataOutputStream(outNew));
        JobTokenIdentifier jobTokenIdentifier1 = new JobTokenIdentifier();
        jobTokenIdentifier1.readFields(new DataInputStream(new ByteArrayInputStream(outNew.toByteArray())));
        System.out.println("new - 3");
        System.out.println(jobTokenIdentifier1.getJobId());

//        Text mapReduceShuffleToken = new Text("MapReduceShuffleToken");
//        credentials.addSecretKey(mapReduceShuffleToken, shufflePassword);
//        credentials.write(new DataOutputStream(System.out));

//
////        cred.addToken(token.getService(), token);
////        cred.writeTokenStorageFile(tokenFile, conf,
////                Credentials.SerializedFormat.WRITABLE);
//
//        Configuration job = new Configuration(false);
//        job.addResource(getClass().getClassLoader().getResourceAsStream("test.xml"));
//        UserGroupInformation.setConfiguration(job);
//        SecurityUtil.setConfiguration(job);
//
////        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
//
////        JobConf conf = new JobConf();
////
////        Path tokenFilePath = new Path("testDir", "tokens-file");
////        Map<String, String> newEnv = new HashMap<>();
////        newEnv.put(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION, tokenFilePath.toUri().getPath());
////        credentials.writeTokenStorageFile(tokenFilePath, conf);
//
//        credentials.write(new DataOutputStream(System.out));
////        credentials.writeTokenStorageFile()
//        LOG.info("{}", UserGroupInformation.getCurrentUser());
//        LOG.info("Executing with tokens: {}", credentials.getAllTokens());
//
//        // Create TaskUmbilicalProtocol as actual task owner.
//        Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
//        System.err.println(jt);
    }

//    @Test
    public void other() {

        String jobId = "zewx";
        SecurityUtil.setTokenServiceUseIp(false);
        JobTokenSecretManager jobTokenSecretManager = new JobTokenSecretManager();
        JobTokenIdentifier tokenId = new JobTokenIdentifier(new Text(jobId));
        Token<JobTokenIdentifier> jobToken = new Token<>(tokenId, jobTokenSecretManager);
        jobTokenSecretManager.addTokenForJob(jobId, jobToken);

        SecurityUtil.setTokenService(jobToken, InetSocketAddress.createUnresolved("virtual", 50020)); // TODO
        LOG.info("Service address for token is " + jobToken.getService());

        Credentials credentials = new Credentials();
        credentials.addToken(jobToken.getService(), jobToken);
        TokenCache.setJobToken(jobToken, credentials);

        Text mapReduceShuffleToken = new Text("MapReduceShuffleToken");
        credentials.addSecretKey(mapReduceShuffleToken, jobToken.getPassword());

        LOG.info("Executing with {} tokens: {}", credentials.getAllTokens().size(), credentials.getAllTokens());
        LOG.info("and with {} secrets: {}", credentials.getAllSecretKeys().size(), credentials.getSecretKeyMap());
    }
}