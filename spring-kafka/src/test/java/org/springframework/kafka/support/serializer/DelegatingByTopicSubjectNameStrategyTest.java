package org.springframework.kafka.support.serializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class DelegatingByTopicSubjectNameStrategyTest {

  private static final DummyStrategy DUMMY_STRATEGY = new DummyStrategy();
  private static final OtherStrategy OTHER_STRATEGY = new OtherStrategy();

  static class DummyStrategy implements SubjectNameStrategy {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema parsedSchema) {
      return "dummy:" + topic;
    }
  }

  static class OtherStrategy implements SubjectNameStrategy {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public String subjectName(String topic, boolean isKey, ParsedSchema parsedSchema) {
      return "other:" + topic;
    }
  }

  static class NotAStrategy {

  }

  @Test
  void testDefaultDelegateIsUsedWhenNoPatternMatches() {
    var strategy = new DelegatingByTopicSubjectNameStrategy(DUMMY_STRATEGY,
        Map.of(Pattern.compile("special-topic-.*"), OTHER_STRATEGY));
    var result = strategy.subjectName("unknown-topic", false, null);
    assertEquals("dummy:unknown-topic", result);
  }

  @Test
  void testPatternDelegateIsUsed() {
    var strategy = new DelegatingByTopicSubjectNameStrategy(DUMMY_STRATEGY,
        Map.of(Pattern.compile("special-topic-.*"), OTHER_STRATEGY));
    var result = strategy.subjectName("special-topic-123", false, null);
    assertEquals("other:special-topic-123", result);
  }

  @Test
  void testEmptyPatternMapUsesDefault() {
    var strategy = new DelegatingByTopicSubjectNameStrategy(DUMMY_STRATEGY, Map.of());
    var result = strategy.subjectName("any-topic", false, null);
    assertEquals("dummy:any-topic", result);
  }

  @Test
  void testConfigureWithStringMapping() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    strategy.configure(Map.of(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT, DummyStrategy.class.getName(),
        DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_CONFIG, "special-topic-.*:" + OtherStrategy.class.getName()));
    var result = strategy.subjectName("special-topic-abc", false, null);
    assertEquals("other:special-topic-abc", result);
    assertEquals("dummy:other-topic", strategy.subjectName("other-topic", false, null));
  }

  @Test
  void testConfigureWithMapMapping() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    strategy.configure(Map.of(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT, DummyStrategy.class.getName(),
        DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_CONFIG, Map.of("special-topic-.*", OtherStrategy.class.getName())));
    var result = strategy.subjectName("special-topic-xyz", false, null);
    assertEquals("other:special-topic-xyz", result);
  }

  @Test
  void testThrowsOnDuplicatePattern() {
    var configMap = Map.of(Pattern.compile("topic-.*"), DUMMY_STRATEGY, Pattern.compile("topic-.*"),
        OTHER_STRATEGY);
    assertThrows(IllegalArgumentException.class, () -> new DelegatingByTopicSubjectNameStrategy(DUMMY_STRATEGY, configMap));
  }

  @Test
  void testThrowsIfNoDefaultConfigured() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    Map<String, Object> configMap = Map.of();
    assertThrows(IllegalArgumentException.class, () -> strategy.configure(configMap));
  }

  @Test
  void testInvalidPatternFormatInStringMapping() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    var configMap = Map.of(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT,
        DummyStrategy.class.getName(),
        DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_CONFIG, "invalidformat" // missing colon
    );
    assertThrows(IllegalArgumentException.class, () -> strategy.configure(configMap));
  }

  @Test
  void testInvalidDelegateClassName() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    var configMap = Map.of(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT,
        "non.existent.ClassName");
    assertThrows(IllegalArgumentException.class, () -> {
      strategy.configure(configMap);
    });
  }

  @Test
  void testNullValueInMappingThrows() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    var configMap = new HashMap<String, String>();
    configMap.put(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT, DummyStrategy.class.getName());
    configMap.put(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_CONFIG, null);
    assertThrows(IllegalArgumentException.class, () -> strategy.configure(configMap));
  }

  @Test
  void testUnsupportedTypeForTopicConfigThrows() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    var configMap = new HashMap<String, Object>();
    configMap.put(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT, DummyStrategy.class.getName());
    configMap.put(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_CONFIG, 42); // unsupported type
    assertThrows(IllegalArgumentException.class, () -> strategy.configure(configMap));
  }

  @Test
  void testCacheIsClearedAfterReconfiguration() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    strategy.configure(Map.of(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT, DummyStrategy.class.getName(),
        DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_CONFIG, "special-topic-.*:" + OtherStrategy.class.getName()));
    // First call caches OtherStrategy
    var result1 = strategy.subjectName("special-topic-abc", false, null);
    assertEquals("other:special-topic-abc", result1);

    // Reconfigure to use DummyStrategy for all topics
    strategy.configure(Map.of(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT, DummyStrategy.class.getName()));
    var result2 = strategy.subjectName("special-topic-abc", false, null);
    assertEquals("dummy:special-topic-abc", result2);
  }

  @Test
  void testCreateDelegateThrowsIfNotSubjectNameStrategy() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    var configMap = Map.of(DelegatingByTopicSubjectNameStrategy.VALUE_SERIALIZATION_TOPIC_DEFAULT,
        NotAStrategy.class.getName());
    assertThrows(IllegalArgumentException.class, () -> strategy.configure(configMap));
  }

  @Test
  void testSubjectNameThrowsIfNotConfigured() {
    var strategy = new DelegatingByTopicSubjectNameStrategy();
    assertThrows(IllegalStateException.class, () -> strategy.subjectName("topic", false, null));
  }
}