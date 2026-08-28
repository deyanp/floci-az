package io.floci.az.services.eventhub;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArtemisConfigGeneratorTest {

    private static final Pattern DIVERT_NAME = Pattern.compile("<divert name=\"([^\"]+)\"");

    private static List<String> divertNames(String namespace, Map<String, List<String>> entities) {
        String brokerXml = new ArtemisConfigGenerator(null).generate(namespace, entities);
        List<String> names = new ArrayList<>();
        Matcher matcher = DIVERT_NAME.matcher(brokerXml);
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names;
    }

    private static void assertAllDistinct(List<String> names) {
        Set<String> distinct = new HashSet<>(names);
        assertEquals(names.size(), distinct.size(),
                "divert names collide, so Artemis would skip a binding: " + names);
    }

    /**
     * Azure allows a dot in a hub name and Artemis names should not carry one, so two hubs that
     * differ only there would reduce to the same divert name.
     */
    @Test
    void hubNamesThatSanitizeAlikeKeepDistinctDivertNames() {
        assertAllDistinct(divertNames("ns", Map.of(
                "eh.1", List.of("$Default"),
                "eh-1", List.of("$Default"))));
    }

    /**
     * The separator joining the parts of a divert name is itself legal inside a hub or consumer
     * group name, so the parts have to stay distinguishable after they are joined.
     */
    @Test
    void namesThatJoinToTheSameStringKeepDistinctDivertNames() {
        assertAllDistinct(divertNames("ns", Map.of(
                "a-to-b", List.of("c"),
                "a", List.of("b-to-c"))));
    }

    /** The generated file must not change between runs, or every namespace restart rewrites it. */
    @Test
    void generatingTwiceProducesTheSameNames() {
        Map<String, List<String>> entities = Map.of("eh1", List.of("$Default", "my-consumer-group"));
        assertEquals(divertNames("ns", entities), divertNames("ns", entities));
    }
}
