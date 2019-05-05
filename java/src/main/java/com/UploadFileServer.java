package com;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

import com.tzutalin.grpc.blobkeeper.BlobKeeperGrpc;
import com.tzutalin.grpc.blobkeeper.PutRequest;
import com.tzutalin.grpc.blobkeeper.PutResponse;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

public class UploadFileServer {
    private static final Logger logger = Logger.getLogger(UploadFileServer.class.getName());
    private static final int PORT = 50051;

    private Server mServer;

    private void start() throws IOException {
    /* The port on which the mServer should run UploadFileServer*/

        mServer = ServerBuilder.forPort(PORT)
                .addService(new UploadFileServer.BlobKeeperImpl())
                .build()
                .start();
        System.out.println("****  Server started, listening on " + PORT);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.out.println("*** shutting down gRPC mServer since JVM is shutting down");
                UploadFileServer.this.stop();
                System.out.println("*** mServer shut down");
            }
        });
    }

    private void stop() {
        if (mServer != null) {
            mServer.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (mServer != null) {
            mServer.awaitTermination();
        }
    }

    /**
     * Main launches the mServer from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        
        final UploadFileServer server = new UploadFileServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class BlobKeeperImpl extends BlobKeeperGrpc.BlobKeeperImplBase {
        private int mStatus = 200;
        private String mMessage = "";
        private BufferedOutputStream mBufferedOutputStream = null;

        @Override
        public StreamObserver<PutRequest> getBlob(final StreamObserver<PutResponse> responseObserver) {
            return new StreamObserver<PutRequest>() {
                int mmCount = 0;                
                public void onNext(PutRequest request) {
                	 mmCount++;
                    // Print count
                    System.out.println("onNext count: *** " + mmCount);
                    
                   

                    byte[] data = request.getData().toByteArray();
                    long offset = request.getOffset();
                    String name = "SampleJPGImage_30mbmb.jpg";
                    
                    
                    System.out.println("onNext count: " + data.toString());
                    System.out.println("onNext count: " + offset);
                    System.out.println("onNext count: " + name);
                    try {
                        if (mBufferedOutputStream == null) {
                            mBufferedOutputStream = new BufferedOutputStream(new FileOutputStream("receive_" + name));
                        }
                        mBufferedOutputStream.write(data);
                        mBufferedOutputStream.flush();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(PutResponse.newBuilder().setStatus(mStatus).setMessage(mMessage).build());
                    responseObserver.onCompleted();
                    if (mBufferedOutputStream != null) {
                        try {
                            mBufferedOutputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            mBufferedOutputStream = null;
                        }
                    }
                }
            };
        }
    }
}
