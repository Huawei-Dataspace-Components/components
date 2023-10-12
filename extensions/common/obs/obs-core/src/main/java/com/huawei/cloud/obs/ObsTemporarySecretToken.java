package com.huawei.cloud.obs;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.eclipse.edc.connector.transfer.spi.types.SecretToken;

public record ObsTemporarySecretToken(@JsonProperty(value = "access") String access,
                                      @JsonProperty(value = "secret") String secret,
                                      @JsonProperty(value = "securitytoken") String securityToken,
                                      @JsonProperty("expiration") long expiration) implements SecretToken {

    @Override
    public long getExpiration() {
        return expiration;
    }

}
