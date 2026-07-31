package io.floci.az.core.docker;

import io.floci.az.config.EmulatorConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("ContainerStorageHelper — naming and labels")
class ContainerStorageHelperTest {

    @Test
    void namesCarryTheAzPrefixWithoutNamespace() {
        assertEquals("floci-az-pg-server1", ContainerStorageHelper.dockerName(config(""), "pg-server1"));
        assertEquals("floci-az-aks-abc123",
                ContainerStorageHelper.resourceName(config(""), "aks", null, "abc123"));
        assertEquals("floci-az-aks-vol9",
                ContainerStorageHelper.resourceName(config(""), "aks", "vol9", "abc123"));
    }

    @Test
    void nullConfigYieldsDefaultModeNames() {
        assertEquals("floci-az-pg-server1", ContainerStorageHelper.dockerName(null, "pg-server1"));
    }

    @Test
    void namespaceLandsBetweenCloudAndServiceTokens() {
        assertEquals("floci-az-run-one-pg-server1",
                ContainerStorageHelper.dockerName(config("run-one"), "pg-server1"));
    }

    @Test
    void alreadyPrefixedNamesAreNormalized() {
        assertEquals("floci-az-pg-x", ContainerStorageHelper.dockerName(config(""), "floci-az-pg-x"));
        assertEquals("floci-az-pg-x", ContainerStorageHelper.dockerName(config(""), "floci-pg-x"));
        assertEquals("floci-az-run-one-pg-x",
                ContainerStorageHelper.dockerName(config("run-one"), "floci-az-pg-x"));
    }

    @Test
    void defaultLabelsIdentifyThisEmulator() {
        assertEquals(
                Map.of("floci", "true", "floci_emulator", "floci-az"),
                ContainerStorageHelper.defaultLabels(config("")));
        assertEquals(
                Map.of("floci", "true", "floci_emulator", "floci-az", "floci_namespace", "run-one"),
                ContainerStorageHelper.defaultLabels(config(" run/one ")));
        assertEquals(
                Map.of("floci", "true", "floci_emulator", "floci-az"),
                ContainerStorageHelper.defaultLabels(null));
    }

    @Test
    void unsafeNamespaceSegmentsAreIgnored() {
        assertEquals("floci-az-pg-x", ContainerStorageHelper.dockerName(config(".."), "pg-x"));
        assertEquals(
                Map.of("floci", "true", "floci_emulator", "floci-az"),
                ContainerStorageHelper.defaultLabels(config("..")));
    }

    private static EmulatorConfig config(String namespace) {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.DockerConfig docker = mock(EmulatorConfig.DockerConfig.class);
        when(config.docker()).thenReturn(docker);
        when(docker.resourceNamespace()).thenReturn(
                namespace.isBlank() ? Optional.empty() : Optional.of(namespace));
        return config;
    }
}
