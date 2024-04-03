package org.opensearch.flint.core.auth;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.signer.Signer;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.http.protocol.HttpCoreContext.HTTP_TARGET_HOST;
import static org.opensearch.flint.core.auth.AWSRequestSigningApacheInterceptor.nvpToMapParams;
import static org.opensearch.flint.core.auth.AWSRequestSigningApacheInterceptor.skipHeader;

public class AWSRequestSigV4ASigningApacheInterceptor implements HttpRequestInterceptor {
    private final String service;

    private final String region;

    /**
     * The particular signer implementation.
     */
    private final Signer signer;

    /**
     * The source of AWS credentials for signing.
     */
    private final AWSCredentialsProvider awsCredentialsProvider;

    private final static String HTTPS_PROTOCOL = "https";
    private final static int HTTPS_PORT = 443;

    public AWSRequestSigV4ASigningApacheInterceptor(final String service,
                                              final String region,
                                              final Signer signer,
                                              final AWSCredentialsProvider awsCredentialsProvider) {
        this.service = service;
        this.region = region;
        this.signer = signer;
        this.awsCredentialsProvider = awsCredentialsProvider;
    }

    @Override
    public void process(HttpRequest request, HttpContext context) throws IOException {
        URIBuilder uriBuilder;
        try {
            uriBuilder = new URIBuilder(request.getRequestLine().getUri());
        } catch (URISyntaxException e) {
            throw new IOException("Invalid URI" , e);
        }

        SdkHttpFullRequest.Builder builder = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.fromValue(request.getRequestLine().getMethod()))
                .port(HTTPS_PORT)
                .protocol(HTTPS_PROTOCOL)
                .host((String) context.getAttribute(HTTP_TARGET_HOST))
                .headers(headerArrayToMap(request.getAllHeaders()))
                .rawQueryParameters(nvpToMapParams(uriBuilder.getQueryParams()));

        try {
            builder.encodedPath(uriBuilder.build().getRawPath());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        AWSSessionCredentials awsSessionCredentials = (AWSSessionCredentials) awsCredentialsProvider.getCredentials();
        AwsSessionCredentials awsCredentials = new AwsSessionCredentials.Builder()
                .accessKeyId(awsSessionCredentials.getAWSAccessKeyId())
                .secretAccessKey(awsSessionCredentials.getAWSSecretKey())
                .sessionToken(awsSessionCredentials.getSessionToken())
                .build();
        ExecutionAttributes executionAttributes = new ExecutionAttributes();
        executionAttributes.putAttribute(AwsSignerExecutionAttribute.AWS_CREDENTIALS,
                awsCredentials);
        executionAttributes.putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME,
                service);
        executionAttributes.putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION,
                Region.of(region));

        SdkHttpFullRequest signedRequest = signer.sign(builder.build(), executionAttributes);

        request.setHeaders(convertHeaderMapToArray(signedRequest.headers()));
    }

    static Map<String, List<String>> headerArrayToMap(final Header[] headers) {
        Map<String, List<String>> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Header header : headers) {
            if (!skipHeader(header)) {
                headersMap.put(header.getName(), Collections.singletonList(header.getValue()));
            }
        }
        return headersMap;
    }

    public Header[] convertHeaderMapToArray(final Map<String, List<String>> mapHeaders) {
        Header[] headers = new Header[mapHeaders.size()];
        int i = 0;
        for (Map.Entry<String, List<String>> h : mapHeaders.entrySet()) {
            headers[i++]  = (new BasicHeader(h.getKey(), String.join(",",h.getValue())));
        }
        return headers;
    }
}
