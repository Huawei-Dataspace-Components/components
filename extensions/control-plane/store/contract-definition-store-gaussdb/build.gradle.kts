plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)
    api(libs.edc.util)
    implementation(libs.failsafe.core)
    // sql libs
    implementation(libs.edc.sql.contractdefstore)
    implementation(libs.edc.sql.core)
//    implementation(libs.edc.spi.datasource.transaction)

    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(libs.edc.spi.contract))
    testImplementation(testFixtures(project(":extensions:common:gaussdb:gaussdb-test")))

}
