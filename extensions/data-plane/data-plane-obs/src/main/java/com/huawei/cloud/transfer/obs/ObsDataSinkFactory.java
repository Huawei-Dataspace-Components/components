package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.transfer.obs.validation.ObsDataAddressValidationRule;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.connector.dataplane.util.validation.ValidationRule;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

public class ObsDataSinkFactory extends ObsFactory implements DataSinkFactory {

    private static final int CHUNK_SIZE_BYTES = 1024 * 1024 * 500; //transfer 500mb at a time
    private final ValidationRule<DataAddress> validation = new ObsDataAddressValidationRule();
    private final Monitor monitor;
    private final ExecutorService executorService;

    public ObsDataSinkFactory(Vault vault, TypeManager typeManager, Monitor monitor, ExecutorService executorService) {
        super(vault, typeManager);
        this.monitor = monitor;
        this.executorService = executorService;
    }

    @Override
    public boolean canHandle(DataFlowRequest request) {
        return ObsBucketSchema.TYPE.equals(request.getDestinationDataAddress().getType());
    }

    @Override
    public DataSink createSink(DataFlowRequest request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(validationResult.getFailureDetail());
        }

        var destination = request.getDestinationDataAddress();

        var obsClient = createObsClient(destination);
        return ObsDataSink.Builder.newInstance()
                .bucketName(destination.getStringProperty(ObsBucketSchema.BUCKET_NAME))
                .objectName(destination.getKeyName())
                .monitor(monitor)
                .executorService(executorService)
                .requestId(request.getId())
                .client(obsClient)
                .chunkSizeBytes(CHUNK_SIZE_BYTES)
                .build();
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        return validation.apply(request.getDestinationDataAddress());
    }


}
