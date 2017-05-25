package com.qiniu.android.collect;


import android.content.Context;
import android.util.Log;

import com.qiniu.android.http.UserAgent;
import com.qiniu.android.storage.UpToken;
import com.qiniu.android.utils.ContextGetter;

import org.chromium.net.CronetEngine;
import org.chromium.net.CronetException;
import org.chromium.net.UploadDataProvider;
import org.chromium.net.UploadDataProviders;
import org.chromium.net.UrlRequest;
import org.chromium.net.UrlResponseInfo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * 收集上传信息，发送到后端
 */
public class UploadInfoCollector {
    /**
     * 任务队列
     */
    private static ThreadPoolExecutor singleServer = null;
    private static ExecutorService crontReqExecutor = null;
    private static ExecutorService crontResExecutor = null;

    private static CronetEngine cronetEngine;

    private final String serverURL;
    private final String recordFileName;
    private File recordFile = null;
    private long lastUpload;// milliseconds

    private static UploadInfoCollector httpCollector;
    private static UploadInfoCollector uploadCollector;

    private static UploadInfoCollector getHttpCollector() {
        if (httpCollector == null) {
            httpCollector = new UploadInfoCollector("_qiniu_record_file_hu3z9lo7anx03", Config.serverURL);
        }
        return httpCollector;
    }

    private static UploadInfoCollector getUploadCollector() {
        if (uploadCollector == null) {
            uploadCollector = new UploadInfoCollector("_qiniu_record_file_upm6xola4sk3", Config.serverURL2);
        }
        return uploadCollector;
    }


    private UploadInfoCollector(String recordFileName, String serverURL) {
        this.recordFileName = recordFileName;
        this.serverURL = serverURL;
        try {
            reset0();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 清理操作。
     * 若需更改 isRecord、isUpload 的值，请在此方法调用前修改。
     */
    public static void clean() {
        try {
            if (singleServer != null) {
                singleServer.shutdown();
            }
        } catch (Exception e) {
            // do nothing
        }
        try {
            if (crontReqExecutor != null) {
                crontReqExecutor.shutdown();
            }
        } catch (Exception e) {
            // do nothing
        }
        try {
            if (crontResExecutor != null) {
                crontResExecutor.shutdown();
            }
        } catch (Exception e) {
            // do nothing
        }
        singleServer = null;
        crontReqExecutor = null;
        crontResExecutor = null;

        singleServer = null;
        cronetEngine = null;
        try {
            getHttpCollector().clean0();
        } catch (Exception e) {
            e.printStackTrace();
        }
        httpCollector = null;
        try {
            getUploadCollector().clean0();
        } catch (Exception e) {
            e.printStackTrace();
        }
        uploadCollector = null;

    }


    private void clean0() {
        try {
            if (recordFile != null) {
                recordFile.delete();
            } else {
                new File(getRecordDir(Config.recordDir), recordFileName).delete();
            }
        } catch (Exception e) {
            // do nothing
        }
        recordFile = null;
    }

    /**
     * 修改记录"是否记录上传信息: isRecord","记录信息所在文件夹: recordDir"配置后,调用此方法重置.
     * 上传方式, 时间间隔,文件最大大小,上传阀值等参数修改不用调用此方法.
     */
    public static void reset() {
        try {
            getHttpCollector().reset0();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            getUploadCollector().reset0();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void reset0() throws IOException {
        if (Config.isRecord) {
            initRecordFile(getRecordDir(Config.recordDir));
        }
        if (!Config.isRecord && singleServer != null) {
            singleServer.shutdown();
        }
        if (Config.isRecord && (singleServer == null || singleServer.isShutdown())) {
            singleServer = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>());
        }
    }

    private File getRecordDir(String recordDir) {
        return new File(recordDir);
    }


    private void initRecordFile(File recordDir) throws IOException {
        if (recordDir == null) {
            throw new IOException("record's dir is not setted");
        }
        if (!recordDir.exists()) {
            boolean r = recordDir.mkdirs();
            if (!r) {
                throw new IOException("mkdir failed: " + recordDir.getAbsolutePath());
            }
            return;
        }
        if (!recordDir.isDirectory()) {
            throw new IOException(recordDir.getAbsolutePath() + " is not a dir");
        }

        recordFile = new File(recordDir, recordFileName);
    }


    public static void handleHttp(final UpToken upToken, final RecordMsg record) {
        try {
            if (Config.isRecord) {
                getHttpCollector().handle0(upToken, record);
            }
        } catch (Throwable t) {
            // do nothing
        }
    }

    public static void handleUpload(final UpToken upToken, final RecordMsg record) {
        try {
            if (Config.isRecord) {
                getUploadCollector().handle0(upToken, record);
            }
        } catch (Throwable t) {
            // do nothing
        }
    }

    private void handle0(final UpToken upToken, final RecordMsg record) {
        if (singleServer != null && !singleServer.isShutdown() && singleServer.getQueue().size() < 15) {
            Runnable taskRecord = new Runnable() {
                @Override
                public void run() {
                    if (Config.isRecord) {
                        try {
                            tryRecode(record.toRecordMsg(), recordFile);
                        } catch (Throwable t) {
                            // do nothing
                        }
                    }
                }
            };
            singleServer.submit(taskRecord);

            // 少几次上传没有影响
            if (Config.isUpload && upToken != UpToken.NULL) {
                Runnable taskUpload = new Runnable() {
                    @Override
                    public void run() {
                        if (Config.isRecord && Config.isUpload) {
                            try {
                                tryUploadAndClean(upToken, recordFile);
                            } catch (Throwable t) {
                                // do nothing
                            }
                        }
                    }
                };
                singleServer.submit(taskUpload);
            }
        }
    }


    private void tryRecode(String msg, File recordFile) {
        if (Config.isRecord && recordFile.length() < Config.maxRecordFileSize) {
            // 追加到文件尾部并换行
            writeToFile(recordFile, msg + "\n", true);
        }
    }

    private void tryUploadAndClean(final UpToken upToken, File recordFile) {
        if (Config.isUpload && recordFile.length() > Config.uploadThreshold) {
            long now = new Date().getTime();
            // Config.interval 单位为：分钟
            if (now > (lastUpload + Config.interval * 60 * 1000)) {
                lastUpload = now;
                try {
                    //同步上传
                    boolean success = upload(upToken, recordFile);
                    if (success) {
                        // 记录文件重置为空
                        writeToFile(recordFile, "", false);
                        writeToFile(recordFile, "", false);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void writeToFile(File file, String msg, boolean isAppend) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file, isAppend);
            fos.write(msg.getBytes(Charset.forName("UTF-8")));
            fos.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    // do nothing
                }
            }
        }
    }

    //同步上传
    private boolean upload(final UpToken upToken, final File recordFile) {
        getCronetEngine(null);

        final CountDownLatch signal = new CountDownLatch(1);
        final boolean[] success = {false};

        UploadDataProvider uploadDataProvider = UploadDataProviders.create(recordFile);
        UrlRequest.Callback callback = new UrlRequest.Callback() {
            @Override
            public void onCanceled(UrlRequest request, UrlResponseInfo info) {
                signal.countDown();
            }

            @Override
            public void onRedirectReceived(UrlRequest request, UrlResponseInfo info, String newLocationUrl) throws Exception {
                // 是否需要跟随 302
                request.cancel();
                signal.countDown();
            }


            @Override
            public void onResponseStarted(UrlRequest request, UrlResponseInfo info) throws Exception {
                request.read(ByteBuffer.allocateDirect(1024));
            }


            @Override
            public void onReadCompleted(UrlRequest request, UrlResponseInfo info, ByteBuffer byteBuffer) throws Exception {
                // 丢弃响应体
                byteBuffer.clear();
                request.read(byteBuffer);
            }


            @Override
            public void onSucceeded(UrlRequest request, UrlResponseInfo info) {
                Log.d("onSucceeded", "onSucceeded: " + info.getProxyServer() + " --- " + info.getNegotiatedProtocol() + " : " + info);
                try {
                    success[0] = isOk(info);
                } finally {
                    signal.countDown();
                }
            }


            @Override
            public void onFailed(UrlRequest request, UrlResponseInfo info, CronetException error) {
                signal.countDown();
            }
        };

        UrlRequest.Builder builder = cronetEngine.newUrlRequestBuilder(
                serverURL, callback, crontReqExecutor);


        UrlRequest request = builder.
                setUploadDataProvider(uploadDataProvider, crontResExecutor).
                setHttpMethod("POST").
                addHeader("Authorization", "UpToken " + upToken.token).
                addHeader("User-Agent", UserAgent.instance().getUa(upToken.accessKey)).
                addHeader("Content-Type", "text/plain").
                build();
        request.start();

        try {
            signal.await(110, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            request.cancel();
        }

        return success[0];
    }


    private boolean isOk(UrlResponseInfo info) {
        return info.getHttpStatusCode() == 200 &&
                info.getAllHeaders().containsKey("X-Reqid");
    }


    private static synchronized CronetEngine getCronetEngine(Context context) {
        // Lazily create the Cronet engine.
        if (cronetEngine == null) {
            if (context == null) {
                context = ContextGetter.applicationContext();
            }

            CronetEngine.Builder myBuilder = new CronetEngine.Builder(context);
            // disables caching of HTTP data and
            // other information like QUIC server information, HTTP/2 protocol and QUIC protocol.
            cronetEngine = myBuilder
                    .enableHttpCache(CronetEngine.Builder.HTTP_CACHE_DISABLED, 0)
                    .enableHttp2(true)
                    .enableQuic(true)
                    .build();
        }
        if (crontReqExecutor == null || crontReqExecutor.isShutdown()) {
            crontReqExecutor = Executors.newSingleThreadExecutor();
        }
        if (crontResExecutor == null || crontResExecutor.isShutdown()) {
            crontResExecutor = Executors.newSingleThreadExecutor();
        }
        return cronetEngine;
    }


    public static abstract class RecordMsg {
        public abstract String toRecordMsg();
    }

}
