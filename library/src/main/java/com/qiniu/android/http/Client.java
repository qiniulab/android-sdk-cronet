package com.qiniu.android.http;

import android.content.Context;
import android.util.Log;

import com.qiniu.android.common.Constants;
import com.qiniu.android.dns.DnsManager;
import com.qiniu.android.storage.UpToken;
import com.qiniu.android.utils.AsyncRun;
import com.qiniu.android.utils.ContextGetter;
import com.qiniu.android.utils.StringMap;
import com.qiniu.android.utils.StringUtils;

import org.chromium.net.CronetEngine;
import org.chromium.net.CronetException;
import org.chromium.net.UploadDataProvider;
import org.chromium.net.UploadDataSink;
import org.chromium.net.UrlRequest;
import org.chromium.net.UrlResponseInfo;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Created by bailong on 15/11/12.
 */
public final class Client {

    private static ExecutorService crontReqExecutor = null;
    private static ExecutorService crontResExecutor = null;

    private static CronetEngine cronetEngine;

    private Context context;

    private final UrlConverter converter;

    private final static int CodePlaceholder = Integer.MAX_VALUE;

    private static String TAG = "Client";

    public Client() {
        this(null, 10, 30, null, null);
    }

    public Client(ProxyConfiguration proxy, int connectTimeout, int responseTimeout, UrlConverter converter, final DnsManager dns) {
        this.converter = converter;
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
            crontReqExecutor = new ThreadPoolExecutor(1, 2,
                    120L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
        }
        if (crontResExecutor == null || crontResExecutor.isShutdown()) {
            crontResExecutor = new ThreadPoolExecutor(1, 3,
                    120L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>());
        }

        // TODO  超时参数怎么设置 ？？ 貌似失败是会等待很久很久
//        builder.connectTimeout(connectTimeout, TimeUnit.SECONDS);
//        builder.readTimeout(responseTimeout, TimeUnit.SECONDS);
//        builder.writeTimeout(0, TimeUnit.SECONDS);
    }



    public void asyncPost(String url, byte[] body, int offset, int size,
                          StringMap headers, final UpToken upToken, ProgressHandler progressHandler,
                          CompletionHandler completionHandler, CancellationHandler c) {
        ClientUploadDataProvider uploadDataProvider;
        if (body != null && body.length > 0) {
            uploadDataProvider = createUploadDataProvider(progressHandler, c, body, offset, size);
        } else {
            uploadDataProvider = createUploadDataProvider(progressHandler, c, new byte[0]);
        }

        asyncSend(url, uploadDataProvider, headers, progressHandler, completionHandler, c, upToken);
    }


    public ResponseInfo syncPost(String url, byte[] body, int offset, int size,
                          StringMap headers, final UpToken upToken, ProgressHandler progressHandler,
                          CompletionHandler completionHandler, CancellationHandler c) {
        ClientUploadDataProvider uploadDataProvider;
        if (body != null && body.length > 0) {
            uploadDataProvider = createUploadDataProvider(progressHandler, c, body, offset, size);
        } else {
            uploadDataProvider = createUploadDataProvider(progressHandler, c, new byte[0]);
        }
        return syncSend(url, uploadDataProvider, headers, progressHandler, completionHandler, c, upToken, 0, TimeUnit.SECONDS);
    }

    public void asyncPost(String url, File file,
                          StringMap headers, final UpToken upToken, ProgressHandler progressHandler,
                          CompletionHandler completionHandler, CancellationHandler c) {
        ClientUploadDataProvider uploadDataProvider = createUploadDataProvider(progressHandler, c, file);

        asyncSend(url, uploadDataProvider, headers, progressHandler, completionHandler, c, upToken);
    }

    public ResponseInfo syncPost(String url, File file,
                          StringMap headers, final UpToken upToken, ProgressHandler progressHandler,
                          CompletionHandler completionHandler, CancellationHandler c) {
        ClientUploadDataProvider uploadDataProvider = createUploadDataProvider(progressHandler, c, file);

        return syncSend(url, uploadDataProvider, headers, progressHandler, completionHandler, c, upToken, 0, TimeUnit.SECONDS);
    }

    public void asyncGet(String url, StringMap headers, final UpToken upToken, CompletionHandler completionHandler) {
        asyncSend(url, null, headers, null, completionHandler, null, upToken);
    }

    public ResponseInfo syncGet(String url, StringMap headers) {
        return syncSend(url, null, headers, null, null, null, null, 0, TimeUnit.SECONDS);
    }


    private static String via(UrlResponseInfo info) {
        String via;
        Map<String, List<String>> headers = info.getAllHeaders();

        if (!(via = header(headers, "X-Via")).equals("")) {
            return via;
        }

        if (!(via = header(headers, "X-Px")).equals("")) {
            return via;
        }

        if (!(via = header(headers, "Fw-Via")).equals("")) {
            return via;
        }
        return via;
    }

    private static String header(Map<String, List<String>> headers, String h) {
        List<String> hs = headers.get(h);
        if (hs == null){
            return "";
        }
        if (hs.size() > 0) {
            return hs.get(0);
        }
        List<String> hsLower = headers.get(h.toLowerCase());
        if (hsLower.size() > 0) {
            return hsLower.get(0);
        }
        return "";
    }


    private static JSONObject buildJsonResp(String str) throws Exception {
        // 允许 空 字符串
        if (StringUtils.isNullOrEmpty(str)) {
            return new JSONObject();
        }
        return new JSONObject(str);
    }


    private static ResponseInfo buildResponseInfo(int aCode, UrlResponseInfo info, ByteArrayOutputStream bytesReceived,
                                                  Throwable ex, String remoteIp, long sent,
                                                  long duration, final UpToken upToken) {
        int code = aCode;
        String reqId = "";
        String xlog = "";
        String xvia = "";
        String ctype = "";
        String protocol = ""; // TODO 协议是否需要加入日志记录 ？？
        String urlStr = "";

        if (info != null) {
            if (aCode == Integer.MAX_VALUE) {
                code = info.getHttpStatusCode();
            }
            reqId = header(info.getAllHeaders(), "X-Reqid");
            reqId = reqId.trim().split(",")[0]; // 服务端偶尔有多个 reqid
            xlog = header(info.getAllHeaders(), "X-Log");
            xvia = via(info);
            ctype = header(info.getAllHeaders(), "Content-Type");
            protocol = info.getNegotiatedProtocol();
            urlStr = info.getUrl();
        }

        URL url = null;
        try {
            url = new URL(urlStr);
        } catch (Exception e) {
            // do nothing
        }
        String host = null;
        int port = -1;
        String path = null;
        String httpProtocol = null;
        if (url != null) {
            host = url.getHost();
            port = url.getPort();
            if (port == -1) {
                port = url.getDefaultPort();
            }
            path = url.getPath();
            httpProtocol = url.getProtocol();
        }

        Log.d(TAG, "buildResponseInfo: " + " : " + protocol + " : " + httpProtocol + " : "  + host + " : " + port + " : " + path + " : " + urlStr);

        String body = null;
        String error = null;
        try {
            body = bytesReceived.toString(Constants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        JSONObject json = null;
        if (body != null && "application/json".equalsIgnoreCase(ctype)) {
            try {
                json = buildJsonResp(body);
                if (code != 200) {;
                    error = json.optString("error", body);
                }
            } catch (Exception e) {
                if (code < 300) {
                    error = e.getMessage();
                }
            }
        } else {
            error = body == null ? "null body" : body;
        }

        if (ex != null) {
            error = ex.getMessage();
        }

        return ResponseInfo.create(json, code, reqId, xlog, xvia, host, path, remoteIp, port, duration, sent, error, upToken);
    }


    private static ResponseInfo onRet(final CompletionHandler complete,
                              int aCode, UrlResponseInfo info, ByteArrayOutputStream bytesReceived,
                              Throwable ex, String remoteIp, long sent,
                              long duration, final UpToken upToken
                              ) {
        final ResponseInfo responseInfo = buildResponseInfo(aCode, info, bytesReceived, ex, remoteIp, sent, duration, upToken);
        Log.d(TAG, "onRet: " + responseInfo);
        Log.d(TAG, "onRet: " + responseInfo.response);
        if (complete != null) {
            AsyncRun.runInMain(new Runnable() {
                @Override
                public void run() {
                    complete.complete(responseInfo, responseInfo.response);
                }
            });
        }
        return responseInfo;
    }

    private void asyncSend(String url, ClientUploadDataProvider uploadDataProvider, StringMap headers,
                           ProgressHandler progressHandler, CompletionHandler completionHandler,
                           CancellationHandler c, UpToken upToken) {
        ClientCallback callback = new ClientCallback(uploadDataProvider, completionHandler, upToken);
        UrlRequest request = buildUrlRequest(url, uploadDataProvider, headers, upToken, callback);
        callback.start = System.currentTimeMillis();
        request.start();
    }

    private boolean hasHeader(StringMap headers, String h) {
        if (headers != null) {
            Object v = headers.get(h);
            if (v != null) {
                return true;
            }
        }
        return false;
    }

    private ResponseInfo syncSend(String url, ClientUploadDataProvider uploadDataProvider, StringMap headers,
                                  ProgressHandler progressHandler, CompletionHandler completionHandler,
                                  CancellationHandler c, UpToken upToken, long timeout, TimeUnit unit) {

        ClientCallback callback = new ClientCallback(uploadDataProvider, completionHandler, upToken, true);
        UrlRequest request = buildUrlRequest(url, uploadDataProvider, headers, upToken, callback);
        callback.start = System.currentTimeMillis();
        request.start();

        ResponseInfo responseInfo = null;

        try {
            if (timeout > 0) {
                responseInfo = callback.getResult(timeout, unit);
            } else {
                responseInfo = callback.getResult();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        request.cancel(); // 会不会再次触发 onCanelled ?  如果用户取消通过异常触发，onCanelled 又怎么处理

        return responseInfo;
    }

    private UrlRequest buildUrlRequest(String url, ClientUploadDataProvider uploadDataProvider,
                                       StringMap headers, UpToken upToken, UrlRequest.Callback callback) {
        if (upToken == null) {
            upToken = UpToken.NULL;
        }
        if (converter != null) {
            url = converter.convert(url);
        }

        final UrlRequest.Builder builder = cronetEngine.newUrlRequestBuilder(
                url, callback, crontReqExecutor);

        if (uploadDataProvider != null) {
            builder.setUploadDataProvider(uploadDataProvider, crontResExecutor);
            builder.setHttpMethod("POST");
        }

        if (headers != null) {
            headers.forEach(new StringMap.Consumer() {
                @Override
                public void accept(String key, Object value) {
                    builder.addHeader(key, value.toString());
                }
            });
        }

        if (!hasHeader(headers, "User-Agent")) {
            builder.addHeader("User-Agent", UserAgent.instance().getUa(upToken.accessKey));
        }
        if (!hasHeader(headers, "Authorization")) {
            builder.addHeader("Authorization", "UpToken " + upToken.token);
        }
        return builder.build();
    }

    private ByteBufferUploadProvider createUploadDataProvider(ProgressHandler progressHandler, CancellationHandler c, byte[] data) {
        return createUploadDataProvider(progressHandler, c, data, 0, data.length);
    }

    // 修改自 UploadDataProviders.create
    private ByteBufferUploadProvider createUploadDataProvider(ProgressHandler progressHandler,
                                                              CancellationHandler c,
                                                              byte[] data, int offset, int length) {
        return new ByteBufferUploadProvider(progressHandler, c, ByteBuffer.wrap(data, offset, length).slice());
    }

    private FileUploadProvider createUploadDataProvider(ProgressHandler progressHandler, CancellationHandler c, final File file) {
        return new FileUploadProvider(progressHandler, c, new FileChannelProvider() {
            @Override
            public FileChannel getChannel() throws IOException {
                return new FileInputStream(file).getChannel();
            }
        });
    }

    private static abstract class ClientUploadDataProvider extends UploadDataProvider {
        abstract long getSentLength();
    }

    private static final class ByteBufferUploadProvider extends ClientUploadDataProvider {
        private final ByteBuffer mUploadBuffer;
        private final ProgressHandler progressHandler;
        private final CancellationHandler c;
        private long sentLength = 0;

        private ByteBufferUploadProvider(ProgressHandler progressHandler, CancellationHandler c, ByteBuffer uploadBuffer) {
            this.mUploadBuffer = uploadBuffer;
            this.progressHandler = progressHandler;
            this.c = c;
        }

        @Override
        public long getSentLength() {
            return sentLength;
        }

        @Override
        public long getLength() {
            return mUploadBuffer.limit();
        }

        @Override
        public void read(UploadDataSink uploadDataSink, ByteBuffer byteBuffer) throws IOException {
            if (c != null && c.isCancelled()) {
                throw new CancellationHandler.CancellationException();
            }
            if (!byteBuffer.hasRemaining()) {
                throw new IllegalStateException("Cronet passed a buffer with no bytes remaining");
            }
            if (byteBuffer.remaining() >= mUploadBuffer.remaining()) {
                long len = mUploadBuffer.remaining(); // byteBuffer.put(src) 会修改 src 的内部尾部位置标记： src.position(src.limit());
                byteBuffer.put(mUploadBuffer);
                sentLength += len;
            } else {
                int oldLimit = mUploadBuffer.limit();
                mUploadBuffer.limit(mUploadBuffer.position() + byteBuffer.remaining());
                long len = mUploadBuffer.remaining();
                byteBuffer.put(mUploadBuffer);
                sentLength += len;
                mUploadBuffer.limit(oldLimit);
            }
            if (progressHandler != null) {
                progressHandler.onProgress(sentLength, getLength());
            }
            uploadDataSink.onReadSucceeded(false);
        }

        @Override
        public void rewind(UploadDataSink uploadDataSink) {
            mUploadBuffer.position(0);
            sentLength = 0;
            uploadDataSink.onRewindSucceeded();
        }
    }

    private interface FileChannelProvider {
        FileChannel getChannel() throws IOException;
    }

    private static final class FileUploadProvider extends ClientUploadDataProvider {
        private volatile FileChannel mChannel;
        private final FileChannelProvider mProvider;
        /** Guards initalization of {@code mChannel} */
        private final Object mLock = new Object();
        private final ProgressHandler progressHandler;
        private final CancellationHandler c;
        private long sentLength = 0;

        private FileUploadProvider(ProgressHandler progressHandler, CancellationHandler c, FileChannelProvider provider) {
            this.progressHandler = progressHandler;
            this.mProvider = provider;
            this.c = c;
        }

        @Override
        public long getSentLength() {
            return sentLength;
        }

        @Override
        public long getLength() throws IOException {
            return getChannel().size();
        }

        @Override
        public void read(UploadDataSink uploadDataSink, ByteBuffer byteBuffer) throws IOException {
            if (c != null && c.isCancelled()) {
                throw new CancellationHandler.CancellationException();
            }
            if (!byteBuffer.hasRemaining()) {
                throw new IllegalStateException("Cronet passed a buffer with no bytes remaining");
            }
            FileChannel channel = getChannel();
            int bytesRead = 0;
            while (bytesRead == 0) {
                int read = channel.read(byteBuffer);
                if (read == -1) {
                    break;
                } else {
                    bytesRead += read;
                }
            }
            sentLength += bytesRead;
            if (progressHandler != null) {
                progressHandler.onProgress(sentLength, getLength());
            }
            uploadDataSink.onReadSucceeded(false);
        }

        @Override
        public void rewind(UploadDataSink uploadDataSink) throws IOException {
            getChannel().position(0);
            sentLength = 0;
            uploadDataSink.onRewindSucceeded();
        }

        /**
         * Lazily initializes the channel so that a blocking operation isn't performed on
         * a non-executor thread.
         */
        private FileChannel getChannel() throws IOException {
            if (mChannel == null) {
                synchronized (mLock) {
                    if (mChannel == null) {
                        mChannel = mProvider.getChannel();
                    }
                }
            }
            return mChannel;
        }

        @Override
        public void close() throws IOException {
            FileChannel channel = mChannel;
            if (channel != null) {
                channel.close();
            }
        }

    }



    private static class ClientCallback extends UrlRequest.Callback {
        private ByteArrayOutputStream bytesReceived = new ByteArrayOutputStream();
        private WritableByteChannel receiveChannel = Channels.newChannel(bytesReceived);

        private final ClientUploadDataProvider senter;

        private final CompletionHandler completionHandler;

        private final UpToken upToken;

        private final int redirectLimit = 5;
        private int redirectCount = 0;

        public long start;
        private long stop;

        final CountDownLatch signal;

        private ResponseInfo responseInfo;

        ClientCallback(ClientUploadDataProvider senter, CompletionHandler completionHandler,
                   UpToken upToken) {
            this(senter, completionHandler, upToken, false);
        }

        ClientCallback(ClientUploadDataProvider senter, CompletionHandler completionHandler,
                           UpToken upToken,
                           boolean sync) {
            this.senter = senter;
            this.completionHandler = completionHandler;
            this.upToken = upToken;
            if (sync) {
                signal = new CountDownLatch(1);
            } else {
                signal = new CountDownLatch(0);
            }
            start = System.currentTimeMillis();
        }

        @Override
        public void onCanceled(UrlRequest request, UrlResponseInfo info) {
            Log.d(TAG, "onCanceled: ");
            stop = System.currentTimeMillis();

            responseInfo = onRet(completionHandler,
                    CodePlaceholder, info, bytesReceived, null, getRemoteIp(),
                    getSentByteCount(), stop - start, upToken);
            signal.countDown();
        }

        @Override
        public void onRedirectReceived(UrlRequest request, UrlResponseInfo info, String newLocationUrl) throws Exception {
            redirectCount++;
            if (redirectCount > redirectLimit) {
                request.cancel();
            } else {
                request.followRedirect();
            }
        }


        @Override
        public void onResponseStarted(UrlRequest request, UrlResponseInfo info) throws Exception {
            request.read(ByteBuffer.allocateDirect(4 * 1024));
        }


        @Override
        public void onReadCompleted(UrlRequest request, UrlResponseInfo info, ByteBuffer byteBuffer) throws Exception {
            Log.d(TAG, "onReadCompleted: 1");
            byteBuffer.flip();
            receiveChannel.write(byteBuffer);
            byteBuffer.clear();
            request.read(byteBuffer);
            Log.d(TAG, "onReadCompleted: 2");
        }


        @Override
        public void onSucceeded(UrlRequest request, UrlResponseInfo info) {
            Log.d(TAG, "onSucceeded: " + info);
            stop = System.currentTimeMillis();

            responseInfo = onRet(completionHandler,
                    CodePlaceholder, info, bytesReceived, null, getRemoteIp(),
                    getSentByteCount(), stop - start, upToken);
            signal.countDown();
        }


        @Override
        public void onFailed(UrlRequest request, UrlResponseInfo info, CronetException error) {
            Log.d(TAG, "onFailed: " + error.getCause());
            stop = System.currentTimeMillis();
            Throwable e = error.getCause();
            if (e == null) {
                e = error;
            }

            int statusCode = ResponseInfo.NetworkError;
            String msg = e.getMessage();
            if (e instanceof CancellationHandler.CancellationException) {
                statusCode = ResponseInfo.Cancelled;
            } else if (e instanceof UnknownHostException) {
                statusCode = ResponseInfo.UnknownHost;
            } else if (msg != null && msg.indexOf("Broken pipe") == 0) {
                statusCode = ResponseInfo.NetworkConnectionLost;
            } else if (e instanceof SocketTimeoutException) {
                statusCode = ResponseInfo.TimedOut;
            } else if (e instanceof java.net.ConnectException) {
                statusCode = ResponseInfo.CannotConnectToHost;
            }

            responseInfo = onRet(completionHandler,
                    statusCode, info, bytesReceived, null, getRemoteIp(),
                    getSentByteCount(), stop - start, upToken);
            signal.countDown();
        }


        public ResponseInfo getResult() throws InterruptedException {
            signal.await();
            return responseInfo;
        }


        public ResponseInfo getResult(long timeout, TimeUnit unit) throws InterruptedException {
            signal.await(timeout, unit);

            return responseInfo;
        }

        // TODO ???
        private String getRemoteIp() {
            return "unkown";
        }


        private long getSentByteCount() {
            if (senter == null) {
                return 0;
            } else {
                return senter.getSentLength();
            }
        }
    }


}
