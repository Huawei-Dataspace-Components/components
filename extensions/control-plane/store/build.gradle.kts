plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
}


dependencies {

    api(project(":extensions:control-plane:store:asset-index-gaussdb"))
    api(project(":extensions:control-plane:store:contract-definition-store-gaussdb"))
    api(project(":extensions:control-plane:store:contract-negotiation-store-gaussdb"))
    api(project(":extensions:control-plane:store:data-plane-instance-store-gaussdb"))
    api(project(":extensions:control-plane:store:policy-definition-store-gaussdb"))
    api(project(":extensions:control-plane:store:policy-monitor-store-gaussdb"))
    api(project(":extensions:control-plane:store:transfer-process-store-gaussdb"))
}


