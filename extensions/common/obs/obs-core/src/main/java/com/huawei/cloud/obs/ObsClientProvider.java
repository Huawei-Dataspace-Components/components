package com.huawei.cloud.obs;

import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsClient;
import org.eclipse.edc.runtime.metamodel.annotation.ExtensionPoint;

@ExtensionPoint
public interface ObsClientProvider {

    /**
     * Returns the client for the specified endpoint, using a global configuration
     */
    ObsClient obsClient(String endpoint);

    /**
     * Returns the client for the specified endpoint, but using the given credentials provider
     *
     * @param endpoint            The OBS endpoint to connect to
     * @param credentialsProvider The credentials provider for authentication
     * @return The ObsClient instance
     */
    ObsClient obsClient(String endpoint, IObsCredentialsProvider credentialsProvider);

    /**
     * Returns the iam client
     */
    IamClient iamClient();

    /**
     * Releases resources used.
     */
    void shutdown();
}
