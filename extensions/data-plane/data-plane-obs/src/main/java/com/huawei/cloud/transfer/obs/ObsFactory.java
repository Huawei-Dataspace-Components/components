/*
 *
 *   Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft
 *
 *   See the NOTICE file(s) distributed with this work for additional
 *   information regarding copyright ownership.
 *
 *   This program and the accompanying materials are made available under the
 *   terms of the Apache License, Version 2.0 which is available at
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations
 *   under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 *
 */

package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsSecretToken;
import com.huawei.cloud.transfer.obs.validation.ObsDataAddressCredentialsValidationRule;
import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.EnvironmentVariableObsCredentialsProvider;
import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import org.eclipse.edc.connector.dataplane.util.validation.ValidationRule;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;

import static com.huawei.cloud.obs.ObsBucketSchema.ACCESS_KEY_ID;
import static com.huawei.cloud.obs.ObsBucketSchema.ENDPOINT;
import static com.huawei.cloud.obs.ObsBucketSchema.SECRET_ACCESS_KEY;

public abstract class ObsFactory {
    private final ValidationRule<DataAddress> credentials = new ObsDataAddressCredentialsValidationRule();
    private final Vault vault;
    private final TypeManager typeManager;

    protected ObsFactory(Vault vault, TypeManager typeManager) {
        this.vault = vault;
        this.typeManager = typeManager;
    }

    protected ObsClient createObsClient(DataAddress dataAddress) {
        var endpoint = dataAddress.getStringProperty(ENDPOINT);
        var secret = vault.resolveSecret(dataAddress.getKeyName());
        IObsCredentialsProvider provider;
        var config = new ObsConfiguration();
        config.setPathStyle(true); //otherwise the bucketname gets prepended
        config.setEndPoint(endpoint);

        if (secret != null) { // AK/SK was stored in vault ->interpret secret as JSON
            var token = typeManager.readValue(secret, ObsSecretToken.class);
            provider = new BasicObsCredentialsProvider(token.ak(), token.sk());
        } else if (credentials.apply(dataAddress).succeeded()) { //AK and SK are stored directly on data address
            var ak = dataAddress.getStringProperty(ACCESS_KEY_ID);
            var sk = dataAddress.getStringProperty(SECRET_ACCESS_KEY);
            provider = new BasicObsCredentialsProvider(ak, sk);
        } else { // no credentials provided, assume there are env vars
            provider = new EnvironmentVariableObsCredentialsProvider();
        }

        return new ObsClient(provider, config);
    }
}
