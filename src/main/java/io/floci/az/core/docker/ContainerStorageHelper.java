package io.floci.az.core.docker;

import io.floci.az.config.EmulatorConfig;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Central helper for Docker resource naming, labelling and child-container volume management.
 *
 * <p>Naming convention (shared across the Floci emulators): every emulator-created container
 * and volume is named {@code floci-az-[<namespace>-]<service>-<id>} and labelled so it is
 * attributable to exactly one emulator by name and by label. The prefix is owned here —
 * call sites pass bare {@code <service>-<id>} tokens, never the prefix.</p>
 *
 * <p>Volume storage modes:</p>
 * <ul>
 *   <li>Named-volume (default) — floci-az manages per-resource Docker named volumes.
 *       Active when {@code FLOCI_AZ_STORAGE_HOST_PERSISTENT_PATH} is not set to an absolute path.</li>
 *   <li>Host-path (legacy) — active when {@code host-persistent-path} is set to an absolute path;
 *       callers use bind-mounts to the specified directory instead.</li>
 * </ul>
 */
public final class ContainerStorageHelper {

    private static final Logger LOG = Logger.getLogger(ContainerStorageHelper.class);

    static final String CLOUD = "az";
    static final String CONTAINER_PREFIX = "floci-" + CLOUD + "-";
    static final String LEGACY_PREFIX = "floci-";

    private ContainerStorageHelper() {}

    /**
     * Canonical volume/container name for a resource.
     * Uses {@code volumeId} when set; falls back to {@code fallbackId} for legacy resources.
     */
    public static String resourceName(EmulatorConfig config, String service, String volumeId, String fallbackId) {
        return dockerName(config, service + "-" + (volumeId != null ? volumeId : fallbackId));
    }

    /**
     * Prefixes {@code baseName} with {@code floci-az-} and the configured resource namespace.
     * Accepts already-prefixed names (current or legacy {@code floci-} prefix) and normalises
     * them, so the namespace always lands between the cloud token and the service token.
     */
    public static String dockerName(EmulatorConfig config, String baseName) {
        String base = stripPrefix(baseName);
        String namespace = resourceNamespace(config);
        if (namespace.isBlank()) {
            return CONTAINER_PREFIX + base;
        }
        return CONTAINER_PREFIX + namespace + "-" + base;
    }

    /**
     * Labels applied to every emulator-created container and volume:
     * {@code floci=true} (umbrella across all Floci emulators),
     * {@code floci_emulator=floci-az} (per-emulator discriminator), and
     * {@code floci_namespace} when a resource namespace is configured.
     */
    public static Map<String, String> defaultLabels(EmulatorConfig config) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("floci", "true");
        labels.put("floci_emulator", "floci-" + CLOUD);
        String namespace = resourceNamespace(config);
        if (!namespace.isBlank()) {
            labels.put("floci_namespace", namespace);
        }
        return labels;
    }

    private static String stripPrefix(String baseName) {
        if (baseName.startsWith(CONTAINER_PREFIX)) {
            return baseName.substring(CONTAINER_PREFIX.length());
        }
        if (baseName.startsWith(LEGACY_PREFIX)) {
            return baseName.substring(LEGACY_PREFIX.length());
        }
        return baseName;
    }

    private static String resourceNamespace(EmulatorConfig config) {
        if (config == null || config.docker() == null || config.docker().resourceNamespace() == null) {
            return "";
        }
        return sanitizeNamePart(config.docker().resourceNamespace().orElse(""));
    }

    private static String sanitizeNamePart(String value) {
        String cleaned = value.trim().replaceAll("[^A-Za-z0-9_.-]+", "-");
        while (cleaned.startsWith("-")) {
            cleaned = cleaned.substring(1);
        }
        while (cleaned.endsWith("-")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1);
        }
        if (cleaned.equals(".") || cleaned.equals("..")) {
            return "";
        }
        return cleaned;
    }

    /**
     * Returns {@code true} when named-volume mode is active.
     * Returns {@code false} only when {@code host-persistent-path} is an absolute path,
     * indicating the caller should use a host bind-mount instead.
     */
    public static boolean isNamedVolumeMode(EmulatorConfig config) {
        return !config.storage().hostPersistentPath().startsWith("/");
    }

    /**
     * Returns whether the given volume should be removed on resource delete,
     * honouring the configured prune policy.
     *
     * - In {@code memory} storage mode: always prune (data cannot survive a restart anyway).
     * - In persistent modes: prune only when {@code prune-volumes-on-delete: true}.
     */
    public static boolean shouldPruneVolume(EmulatorConfig config) {
        return "memory".equals(config.storage().mode()) || config.storage().pruneVolumesOnDelete();
    }

    /**
     * Ensures the host data directory exists for host-path mode (absolute paths only).
     */
    public static void ensureHostDir(String hostDataPath) {
        try {
            Files.createDirectories(Path.of(hostDataPath));
        } catch (IOException e) {
            LOG.errorv("Failed to create data directory {0}: {1}", hostDataPath, e.getMessage());
        }
    }
}
