package com.qiniu.android.storage;

import android.util.Log;

import com.qiniu.android.http.Client;
import com.qiniu.android.http.CompletionHandler;
import com.qiniu.android.http.PostArgs;
import com.qiniu.android.http.ProgressHandler;
import com.qiniu.android.http.ResponseInfo;
import com.qiniu.android.utils.AndroidNetwork;
import com.qiniu.android.utils.Crc32;
import com.qiniu.android.utils.StringMap;
import com.qiniu.android.utils.StringUtils;
import com.qiniu.android.utils.UrlSafeBase64;

import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static java.lang.String.format;


// https://github.com/qbox/product/blob/master/kodo/up.md#putputb64-%E4%B8%8A%E4%BC%A0
final class SimpleUploader {

    private static String TAG = "SimpleUploader";

    /**
     * 上传数据，并以指定的key保存文件
     *
     * @param httpManager       HTTP连接管理器
     * @param data              上传的数据
     * @param key               上传的数据保存的文件名
     * @param token             上传凭证
     * @param completionHandler 上传完成后续处理动作
     * @param options           上传时的可选参数
     */
    static void upload(Client httpManager, Configuration config, byte[] data, String key, UpToken token, final UpCompletionHandler completionHandler,
                       final UploadOptions options) {
        post(data, null, key, token, completionHandler, options, httpManager, config);
    }

    /**
     * 上传文件，并以指定的key保存文件
     *
     * @param client            HTTP连接管理器
     * @param file              上传的文件
     * @param key               上传的数据保存的文件名
     * @param token             上传凭证
     * @param completionHandler 上传完成后续处理动作
     * @param options           上传时的可选参数
     */
    static void upload(Client client, Configuration config, File file, String key, UpToken token, UpCompletionHandler completionHandler,
                       UploadOptions options) {
        post(null, file, key, token, completionHandler, options, client, config);
    }

    private static void post(final byte[] data, final File file, final String key, final UpToken token,
                              final UpCompletionHandler completionHandler,
                              UploadOptions optionsIn, final Client client, final Configuration config) {
        final UploadOptions options = optionsIn != null ? optionsIn : UploadOptions.defaultOptions();
        boolean success = config.zone.preQuery(token.token);
        if (!success) {
            success = config.zone.preQuery(token.token);
            if (!success) {
                ResponseInfo info = ResponseInfo.invalidToken("failed to get up host");
                completionHandler.complete(key, info, info.response);
                return;
            }
        }

        String mime = format(Locale.ENGLISH, "/mimeType/%s", UrlSafeBase64.encodeToString(options.mimeType));

        String keyStr = "";
        if (key != null) {
            keyStr = format("/key/%s", UrlSafeBase64.encodeToString(key));
        }

        String paramStr = "";
        if (options.params.size() != 0) {
            String str[] = new String[options.params.size()];
            int j = 0;
            for (Map.Entry<String, String> i : options.params.entrySet()) {
                str[j++] = format(Locale.ENGLISH, "%s/%s", i.getKey(), UrlSafeBase64.encodeToString(i.getValue()));
            }
            paramStr = "/" + StringUtils.join(str, "/");
        }
        String crc32 = "";
        if (options.checkCrc) {
            long crc = 0;
            if (file != null) {
                try {
                    crc = Crc32.file(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                crc = Crc32.bytes(data);
            }
            crc32 = "/crc32/" + crc;
        }
        long size = 0;
        if (file != null) {
            size = file.length();
        } else {
            size = data.length;
        }

        String fname = "";
        if (file != null) {
            fname = "/fname/" + UrlSafeBase64.encodeToString(file.getName()) + "/x-qn-meta-fname/" + UrlSafeBase64.encodeToString(file.getName());
        }

        final String path = format(Locale.ENGLISH, "put/%d%s%s%s%s%s", size, keyStr, mime, crc32, paramStr, fname);

        final String upHost = config.zone.upHost(token.token, config.useHttps, null);
        String url = String.format("%s/%s", upHost, path);

        final StringMap headers = new StringMap().put("Authorization", "UpToken " + token.token).
                put("Content-Type", "application/octet-stream");

        Log.d(TAG, "post------0: " + path);

        final ProgressHandler progress = new ProgressHandler() {
            @Override
            public void onProgress(long bytesWritten, long totalSize) {
                double percent = ((double)bytesWritten) / totalSize;
                if (percent > 0.95) {
                    percent = 0.95;
                }
                options.progressHandler.progress(key, percent);
            }
        };

        CompletionHandler completion = new CompletionHandler() {

            @Override
            public void complete(ResponseInfo info, JSONObject response) {
                if (info.isNetworkBroken() && !AndroidNetwork.isNetWorkReady()) {
                    options.netReadyHandler.waitReady();
                    if (!AndroidNetwork.isNetWorkReady()) {
                        completionHandler.complete(key, info, response);
                        return;
                    }
                }

                if (info.isOK()) {
                    options.progressHandler.progress(key, 1.0);
                    completionHandler.complete(key, info, response);
                } else if (options.cancellationSignal.isCancelled()) {
                    ResponseInfo i = ResponseInfo.cancelled(token);
                    completionHandler.complete(key, i, null);
                } else if (info.needRetry() || (info.isNotQiniu() && !token.hasReturnUrl())) {
                    final String upHostRetry = config.zone.upHost(token.token, config.useHttps, upHost);
                    final String urlRetry = String.format("%s/%s", upHost, path);
                    Log.d("Qiniu.FormUploader", "retry upload first time use up url " + urlRetry);
                    CompletionHandler retried = new CompletionHandler() {
                        @Override
                        public void complete(ResponseInfo info, JSONObject response) {
                            if (info.isOK()) {
                                options.progressHandler.progress(key, 1.0);
                                completionHandler.complete(key, info, response);
                            } else if (info.needRetry() || (info.isNotQiniu() && !token.hasReturnUrl())) {
                                final String upHostRetry2 = config.zone.upHost(token.token, config.useHttps, upHostRetry);
                                final String urlRetry2 = String.format("%s/%s", upHost, path);
                                Log.d("Qiniu.FormUploader", "retry upload second time use up url " + upHostRetry2);
                                CompletionHandler retried2 = new CompletionHandler() {
                                    @Override
                                    public void complete(ResponseInfo info2, JSONObject response2) {
                                        if (info2.isOK()) {
                                            options.progressHandler.progress(key, 1.0);
                                        } else if (info2.needRetry() || (info2.isNotQiniu() && !token.hasReturnUrl())) {
                                            config.zone.frozenDomain(upHostRetry2);
                                        }
                                        completionHandler.complete(key, info2, response2);
                                    }
                                };
                                if (file != null) {
                                    client.asyncPost(urlRetry2, file, headers, token, progress, retried2, options.cancellationSignal);
                                } else {
                                    client.asyncPost(urlRetry2, data, 0, data.length, headers, token, progress, retried2, options.cancellationSignal);
                                }
                            } else {
                                completionHandler.complete(key, info, response);
                            }
                        }
                    };

                    if (file != null) {
                        client.asyncPost(urlRetry, file, headers, token, progress, retried, options.cancellationSignal);
                    } else {
                        client.asyncPost(urlRetry, data, 0, data.length, headers, token, progress, retried, options.cancellationSignal);
                    }
                }
            }
        };

        if (file != null) {
            client.asyncPost(url, file, headers, token, progress, completion, options.cancellationSignal);
        } else {
            client.asyncPost(url, data, 0, data.length, headers, token, progress, completion, options.cancellationSignal);
        }
    }


    /**
     * 上传数据，并以指定的key保存文件
     *
     * @param client  HTTP连接管理器
     * @param data    上传的数据
     * @param key     上传的数据保存的文件名
     * @param token   上传凭证
     * @param options 上传时的可选参数
     * @return 响应信息 ResponseInfo#response 响应体，序列化后 json 格式
     */
    public static ResponseInfo syncUpload(Client client, Configuration config, byte[] data, String key, UpToken token, UploadOptions options) {
        try {
            return syncUpload0(client, config, data, null, key, token, options);
        } catch (Exception e) {
            return ResponseInfo.create(null, ResponseInfo.UnknownError, "", "", "", "", "", "", 0, 0, 0, e.getMessage(), token);
        }
    }

    /**
     * 上传文件，并以指定的key保存文件
     *
     * @param client  HTTP连接管理器
     * @param file    上传的文件
     * @param key     上传的数据保存的文件名
     * @param token   上传凭证
     * @param options 上传时的可选参数
     * @return 响应信息 ResponseInfo#response 响应体，序列化后 json 格式
     */
    public static ResponseInfo syncUpload(Client client, Configuration config, File file, String key, UpToken token, UploadOptions options) {
        try {
            return syncUpload0(client, config, null, file, key, token, options);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseInfo.create(null, ResponseInfo.UnknownError, "", "", "", "", "", "", 0, 0, 0, e.getMessage(), token);
        }
    }

    private static ResponseInfo syncUpload0(Client client, Configuration config, byte[] data, File file,
                                            final String key, UpToken token, UploadOptions optionsIn) {
        final UploadOptions options = optionsIn != null ? optionsIn : UploadOptions.defaultOptions();
        boolean success = config.zone.preQuery(token.token);
        if (!success) {
            success = config.zone.preQuery(token.token);
            if (!success) {
                return ResponseInfo.invalidToken("failed to get up host");
            }
        }

        String mime = format(Locale.ENGLISH, "/mimeType/%s", UrlSafeBase64.encodeToString(options.mimeType));

        String keyStr = "";
        if (key != null) {
            keyStr = format("/key/%s", UrlSafeBase64.encodeToString(key));
        }

        String paramStr = "";
        if (options.params.size() != 0) {
            String str[] = new String[options.params.size()];
            int j = 0;
            for (Map.Entry<String, String> i : options.params.entrySet()) {
                str[j++] = format(Locale.ENGLISH, "%s/%s", i.getKey(), UrlSafeBase64.encodeToString(i.getValue()));
            }
            paramStr = "/" + StringUtils.join(str, "/");
        }
        String crc32 = "";
        if (options.checkCrc) {
            long crc = 0;
            if (file != null) {
                try {
                    crc = Crc32.file(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                crc = Crc32.bytes(data);
            }
            crc32 = "/crc32/" + crc;
        }

        long size = 0;
        if (file != null) {
            size = file.length();
        } else {
            size = data.length;
        }

        String fname = "";
        if (file != null) {
            fname = "/fname/" + UrlSafeBase64.encodeToString(file.getName()) + "/x-qn-meta-fname/" + UrlSafeBase64.encodeToString(file.getName());
        }

        final String path = format(Locale.ENGLISH, "put/%d%s%s%s%s%s", size, keyStr, mime, crc32, paramStr, fname);

        final String upHost = config.zone.upHost(token.token, config.useHttps, null);
        String url = String.format("%s/%s", upHost, path);

        final StringMap headers = new StringMap().put("Authorization", "UpToken " + token.token).
                put("Content-Type", "application/octet-stream");

        Log.d(TAG, "post0: " + file + fname + "  " + url);

        final ProgressHandler progress = new ProgressHandler() {
            @Override
            public void onProgress(long bytesWritten, long totalSize) {
                double percent = ((double)bytesWritten) / totalSize;
                if (percent > 0.95) {
                    percent = 0.95;
                }
                options.progressHandler.progress(key, percent);
            }
        };

        ResponseInfo responseInfo;
        if (file != null) {
            responseInfo = client.syncPost(url, file, headers, token, progress, null, options.cancellationSignal);
        } else {
            responseInfo = client.syncPost(url, data, 0, data.length, headers, token, progress, null, options.cancellationSignal);
        }

        if (responseInfo.isOK()) {
            return responseInfo;
        }

        //retry for the first time
        if (responseInfo.needRetry() || (responseInfo.isNotQiniu() && !token.hasReturnUrl())) {
            if (responseInfo.isNetworkBroken() && !AndroidNetwork.isNetWorkReady()) {
                options.netReadyHandler.waitReady();
                if (!AndroidNetwork.isNetWorkReady()) {
                    return responseInfo;
                }
            }

            //retry for the second time
            String upHostRetry = config.zone.upHost(token.token, config.useHttps, upHost);
            Log.d("Qiniu.FormUploader", "sync upload retry first time use up host " + upHostRetry);

            if (file != null) {
                responseInfo = client.syncPost(upHostRetry, file, headers, token, progress, null, options.cancellationSignal);
            } else {
                responseInfo = client.syncPost(upHostRetry, data, 0, data.length, headers, token, progress, null, options.cancellationSignal);
            }

            if (responseInfo.needRetry() || (responseInfo.isNotQiniu() && !token.hasReturnUrl())) {
                if (responseInfo.isNetworkBroken() && !AndroidNetwork.isNetWorkReady()) {
                    options.netReadyHandler.waitReady();
                    if (!AndroidNetwork.isNetWorkReady()) {
                        return responseInfo;
                    }
                }

                String upHostRetry2 = config.zone.upHost(token.token, config.useHttps, upHostRetry);
                Log.d("Qiniu.FormUploader", "sync upload retry second time use up host " + upHostRetry2);
                if (file != null) {
                    responseInfo = client.syncPost(upHostRetry2, file, headers, token, progress, null, options.cancellationSignal);
                } else {
                    responseInfo = client.syncPost(upHostRetry2, data, 0, data.length, headers, token, progress, null, options.cancellationSignal);
                }

                if (responseInfo.needRetry() || (responseInfo.isNotQiniu() && !token.hasReturnUrl())) {
                    config.zone.frozenDomain(upHostRetry2);
                }
            }
        }

        return responseInfo;

    }

}
