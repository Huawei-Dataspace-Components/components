package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huawei.cloud.transfer.obs.validation.ObsDataAddressCredentialsValidationRule;
import com.huawei.cloud.transfer.obs.validation.ObsDataAddressValidationRule;
import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.EnvironmentVariableObsCredentialsProvider;
import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.connector.dataplane.util.validation.ValidationRule;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import static com.huawei.cloud.obs.ObsBucketSchema.ACCESS_KEY_ID;
import static com.huawei.cloud.obs.ObsBucketSchema.BUCKET_NAME;
import static com.huawei.cloud.obs.ObsBucketSchema.ENDPOINT;
import static com.huawei.cloud.obs.ObsBucketSchema.KEY_PREFIX;
import static com.huawei.cloud.obs.ObsBucketSchema.SECRET_ACCESS_KEY;

public class ObsDataSourceFactory implements DataSourceFactory {

    private final ValidationRule<DataAddress> validation = new ObsDataAddressValidationRule();
    private final ValidationRule<DataAddress> credentials = new ObsDataAddressCredentialsValidationRule();
    private final Vault vault;
    private final TypeManager typeManager;

    public ObsDataSourceFactory(Vault vault, TypeManager typeManager) {
        this.vault = vault;
        this.typeManager = typeManager;
    }

    @Override
    public boolean canHandle(DataFlowRequest request) {
        return ObsBucketSchema.TYPE.equals(request.getSourceDataAddress().getType());
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var source = request.getSourceDataAddress();

        return ObsDataSource.Builder.newInstance()
                .bucketName(source.getStringProperty(BUCKET_NAME))
                .client(createObsClient(source))
                .keyPrefix(source.getStringProperty(KEY_PREFIX, null))
                .build();
    }


    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        var source = request.getSourceDataAddress();

        return validation.apply(source).map(it -> null);
    }

    private ObsClient createObsClient(DataAddress source) {
        var endpoint = source.getStringProperty(ENDPOINT);
        var secret = vault.resolveSecret(source.getKeyName());
        IObsCredentialsProvider provider;

        if (secret != null) { // AK/SK was stored in vault ->interpret secret as JSON
            var token = typeManager.readValue(secret, ObsSecretToken.class);
            provider = new BasicObsCredentialsProvider(token.ak(), token.sk());
        } else if (credentials.apply(source).succeeded()) { //AK and SK are stored directly on source address
            var ak = source.getStringProperty(ACCESS_KEY_ID);
            var sk = source.getStringProperty(SECRET_ACCESS_KEY);
            provider = new BasicObsCredentialsProvider(ak, sk);
        } else { // no credentials provided, assume there are env vars
            provider = new EnvironmentVariableObsCredentialsProvider();
        }

        return new ObsClient(provider, endpoint);
    }
}
