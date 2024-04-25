plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)
    api(libs.edc.util.lib)
    api(libs.edc.sql.transferprocess)
    api(libs.edc.sql.lease)
    api(libs.huawei.dws.jdbc)

    implementation(libs.failsafe.core)
    implementation(libs.edc.sql.core)

    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(libs.edc.spi.transfer))
    testImplementation(testFixtures(libs.edc.sql.lease))
    testImplementation(testFixtures(project(":extensions:common:gaussdb:gaussdb-core")))

}