import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.PullResponseItem;
import com.github.dockerjava.api.model.WaitResponse;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;

public class Main implements Closeable {

    private static final String DOCKER_COMPOSE = "docker-compose.setup.yml";
    private static final String SETUP_COMMAND = "setup.java";
    private static final String IMAGE = "jbangdev/jbang-action:latest";
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    public static final String WS = "/ws";

    private final DockerClient dockerClient;

    public Main() {
        DockerClientConfig clientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
            .dockerHost(clientConfig.getDockerHost())
            .build();
        dockerClient = DockerClientImpl.getInstance(clientConfig, httpClient);
    }

    @Override
    public void close() throws IOException {
        dockerClient.close();
    }

    private void prepare() throws IOException, URISyntaxException {
        Path absKeysDir = Path.of("data", "keys");
        LOGGER.debug("Checking {}", absKeysDir);
        URL keysDirURL = this.getClass().getResource(absKeysDir.toString());
        if (keysDirURL != null) {
            try (Stream<Path> files = Files.list(Path.of(keysDirURL.toURI()))) {
                files.filter(p -> p.getFileName().toString().endsWith(".pem"))
                    .forEach(p -> {
                        try {
                            LOGGER.debug("Deleting {}", p);
                            Files.delete(p);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
            } finally {
                LOGGER.debug("Deleted .pem files");
            }
        } else {
            LOGGER.info("{} not found", absKeysDir.toAbsolutePath());
        }
    }

    public void pullImage() throws InterruptedException {
        String imageName = IMAGE;
        AtomicBoolean isSuccess = new AtomicBoolean(false);
        List<Image> images = dockerClient.listImagesCmd()
            .withReferenceFilter(imageName)
            .exec();
        boolean containsImage = images.stream()
            .flatMap(i -> Arrays.stream(i.getRepoTags()))
            .anyMatch(s -> s.equals(imageName));
        if (!containsImage) {
            CountDownLatch listLatch = new CountDownLatch(1);
            LOGGER.info("Pulling image \"{}\"", imageName);
            dockerClient.pullImageCmd(imageName)
                .exec(new ResultCallback.Adapter<PullResponseItem>() {

                    private boolean success = false;

                    @Override
                    public void onNext(PullResponseItem res) {
                        LOGGER.debug("{} {}: {}", res.getId(), res.getStatus(),
                            res.getProgressDetail() == null ? "null" : res.getProgressDetail().getCurrent());
                        if (res.isPullSuccessIndicated()) {
                            success = true;
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        throwable.printStackTrace();
                        listLatch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        if (success) {
                            isSuccess.set(true);
                            LOGGER.info("Image \"{}\" pulled.", imageName);
                        }
                        listLatch.countDown();
                    }
                });
            listLatch.await();
            if (!isSuccess.get()) {
                throw new RuntimeException("Image \"" + IMAGE + "\" pull failed");
            }
        } else {
            LOGGER.info("Image \"{}\" present locally.", imageName);
        }
    }

    private String createContainer(Path volumeMount) {
        String containerId = dockerClient.createContainerCmd(IMAGE)
            .withWorkingDir(WS)
            .withCmd(SETUP_COMMAND)
            .withHostConfig(HostConfig.newHostConfig()
                .withBinds(Bind.parse(String.format("%s:%s", volumeMount.toString(), WS))))
            .exec()
            .getId();
        LOGGER.debug("Contaier ID: {}", containerId);
        dockerClient.startContainerCmd(containerId).exec();
        return containerId;
    }

    private int awaitContainerExit(String containerId) throws InterruptedException {
        AtomicInteger status = new AtomicInteger(0);

        final CountDownLatch waitLatch = new CountDownLatch(2);
        dockerClient
            .waitContainerCmd(containerId)
            .exec(new ResultCallback.Adapter<WaitResponse>() {

                private int exitStatus;

                @Override
                public void onNext(WaitResponse res) {
                    exitStatus = res.getStatusCode();
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                    waitLatch.countDown();
                }

                @Override
                public void onComplete() {
                    super.onComplete();
                    waitLatch.countDown();

                    if (exitStatus != 0) {
                        LOGGER.error("Exit Status {}", exitStatus);
                        status.set(exitStatus);
                    } else {
                        LOGGER.info("Exit Status {}", exitStatus);
                    }
                }
            });

        dockerClient.logContainerCmd(containerId)
            .withStdOut(true).withStdErr(true)
            .withFollowStream(true)
            .withTailAll()
            .exec(new ResultCallback.Adapter<Frame>() {
                @Override
                public void onNext(Frame frame) {
                    LOGGER.info(frame.toString());
                }

                @Override
                public void onError(Throwable throwable) {
                    throwable.printStackTrace();
                    waitLatch.countDown();
                }

                @Override
                public void onComplete() {
                    super.onComplete();
                    waitLatch.countDown();
                }
            });
        waitLatch.await();

        return status.get();
    }

    private String runContainer(Path volumeMount) throws InterruptedException {
        String containerId = createContainer(volumeMount);
        int status = awaitContainerExit(containerId);
        if (status != 0) {
            throw new RuntimeException("Container exited with non-zero status.");
        }
        return containerId;
    }

    public void run() throws InterruptedException, IOException, URISyntaxException {
        URL dockerCompose = this.getClass().getResource(DOCKER_COMPOSE);
        if (Objects.nonNull(dockerCompose)) {
            prepare();
            pullImage();
            String containerId = runContainer(Path.of(dockerCompose.getPath()).getParent());
            dockerClient.removeContainerCmd(containerId).exec();
        } else {
            throw new FileNotFoundException(DOCKER_COMPOSE);
        }

    }

    public static void main(String[] args) throws Exception {
        try (Main main = new Main()) {
            main.run();
        }
    }

}
