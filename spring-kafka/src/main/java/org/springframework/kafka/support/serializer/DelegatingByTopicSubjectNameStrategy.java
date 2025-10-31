package org.springframework.kafka.support.serializer;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.springframework.util.Assert;

/**
 * Subject Name Strategy, die basierend auf dem Topic-Namen eine passende SubjectNameStrategy aus einer konfigurierbaren Map auswählt. Falls
 * kein Pattern passt, wird eine Default Strategy verwendet.
 * <p>
 * Muss über den Konfigurationsparmeter {@link AbstractKafkaSchemaSerDeConfig#valueSubjectNameStrategy()} registriert werden. Dazu muss der
 * Key in den Application Properties unter spring.kafka.producer.properties hinzugefügt werden:
 * {@code
 * spring.kafka.producer.properties.value.subject.name.strategy=org.springframework.kafka.support.serializer.DelegatingByTopicSubjectNameStrategy}.
 * Zusätzlich muss dann noch definiert werden, welche Strategy für welches Topic verwendet werden soll.
 * <p>
 * Der Konfigurationsparameter {@link #VALUE_SERIALIZATION_TOPIC_DEFAULT} ist verpflichtend, um die Default Strategy anzugeben, z.B.
 * {@code spring.kafka.subject.name.strategy.bytopic.default: io.confluent.kafka.serializers.subject.TopicNameStrategy}.
 * <p>
 * Über den Konfigurationsparameter {@link #VALUE_SERIALIZATION_TOPIC_CONFIG} kann eine Map im Format {@code selector:class,...} angegeben
 * werden, um pro Topic eine Strategy zu konfigurieren. Z.B.
 * {@code spring.kafka.subject.name.strategy.bytopic.config:
 * my-topic-.*:io.confluent.kafka.serializers.subject.RecordNameStrategy,other-topic:io.confluent.kafka.serializers.subject.TopicRecordNameStrategy}.
 * Bei den Keys können reguläre Ausdrücke verwendet werden.
 *
 */
public class DelegatingByTopicSubjectNameStrategy implements SubjectNameStrategy {

  /**
   * Name des Konfigurationsparameters, der die Subject Name Strategy Selector Map im Format {@code selector:class,...} enthält.
   */
  public static final String VALUE_SERIALIZATION_TOPIC_CONFIG = "spring.kafka.subject.name.strategy.bytopic.config";

  /**
   * Name des Konfigurationsparameters, der den Namen der Default Klasse für die Subject Name Strategy enthält.
   */
  public static final String VALUE_SERIALIZATION_TOPIC_DEFAULT = "spring.kafka.subject.name.strategy.bytopic.default";

  private final AtomicReference<SubjectNameStrategy> defaultDelegate = new AtomicReference<>();
  private final ConcurrentHashMap<Pattern, SubjectNameStrategy> delegates = new ConcurrentHashMap<>();

  private final Map<String, SubjectNameStrategy> delegateCache = new ConcurrentHashMap<>();

  /**
   * Default Konstruktor für die Reflection basierte Initialisierung innerhalb von
   * {@link AbstractKafkaSchemaSerDeConfig#valueSubjectNameStrategy()}.
   */
  public DelegatingByTopicSubjectNameStrategy() {
    super();
  }

  /**
   * Alternativer Konstruktor für eine manuelle Initialisierung.
   *
   * @param defaultDelegate Default SubjectNameStrategy
   * @param delegates       Map mit Pattern und zugehöriger SubjectNameStrategy
   */
  public DelegatingByTopicSubjectNameStrategy(SubjectNameStrategy defaultDelegate, Map<Pattern, SubjectNameStrategy> delegates) {
    Assert.notNull(defaultDelegate, "'defaultDelegate' darf nicht null sein");
    Assert.notNull(delegates, "'delegates' darf nicht null sein");
    var patterns = ConcurrentHashMap.newKeySet();
    delegates.keySet().forEach(pattern -> Assert.isTrue(patterns.add(pattern.pattern()),
        "Doppeltes Pattern erkannt: " + pattern.pattern()));
    this.defaultDelegate.set(defaultDelegate);
    this.delegates.putAll(delegates);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (configs.containsKey(VALUE_SERIALIZATION_TOPIC_DEFAULT)) {
      var value = configs.get(VALUE_SERIALIZATION_TOPIC_DEFAULT);
      if (value instanceof String clazzName) {
        defaultDelegate.set(createDefaultDelegate(clazzName));
      } else {
        throw new IllegalArgumentException(
            String.format("Konfiguration Property '%s' muss ein String sein.", VALUE_SERIALIZATION_TOPIC_DEFAULT));
      }
    } else {
      throw new IllegalArgumentException(
          String.format("Konfiguration Property '%s' muss immer definiert sein.", VALUE_SERIALIZATION_TOPIC_DEFAULT));
    }

    if (configs.containsKey(VALUE_SERIALIZATION_TOPIC_CONFIG)) {
      var value = configs.get(VALUE_SERIALIZATION_TOPIC_CONFIG);
      switch (value) {
        case String stringMapping -> delegates.putAll(processStringMapping(stringMapping));
        case Map<?, ?> configMap -> delegates.putAll(processMap(configMap));
        case null -> throw new IllegalArgumentException(String.format(
            "Konfiguration Property '%s' darf nicht explizit auf null gemappt werden.", VALUE_SERIALIZATION_TOPIC_CONFIG));
        default -> throw new IllegalArgumentException(
            String.format("Konfiguration Property '%s' muss ein String oder eine Map sein, nicht '%s'.", VALUE_SERIALIZATION_TOPIC_CONFIG,
                value.getClass()));
      }
    } else {
      delegates.clear();
    }

    delegateCache.clear();
  }

  @Override
  public String subjectName(String topic, boolean isKey, ParsedSchema parsedSchema) {
    var delegate = findDelegate(topic);
    return delegate.subjectName(topic, isKey, parsedSchema);
  }

  private Map<Pattern, SubjectNameStrategy> processMap(Map<?, ?> mapping) {
    Map<Pattern, SubjectNameStrategy> delegateMap = new HashMap<>();
    mapping.forEach((key, delegate) -> {
      var pattern = obtainPattern(key);
      if (delegateMap.keySet().stream().anyMatch(k -> k.pattern().equals(pattern.pattern()))) {
        throw new IllegalArgumentException("Doppeltes Pattern erkannt: " + pattern.pattern());
      }
      if (delegate instanceof SubjectNameStrategy subjectNameStrategy) {
        delegateMap.put(pattern, subjectNameStrategy);
      } else if (delegate instanceof String clazzName) {
        try {
          delegateMap.put(pattern, createDelegate(clazzName));
        } catch (Exception e) {
          throw new IllegalArgumentException(
              String.format("Fehler beim Erzeugen des Delegates für das Pattern %s: %s", pattern, e.getMessage()), e);
        }
      } else {
        throw new IllegalArgumentException(
            String.format("Es wurde keine unterstützer Delegate Typ für das Pattern %s definiert: %s", pattern, delegate.getClass()));
      }
    });
    return delegateMap;
  }

  private Map<Pattern, SubjectNameStrategy> processStringMapping(String stringMapping) {
    var configMap = new HashMap<>();
    Arrays.stream(stringMapping.split(",")).filter(s -> !s.trim().isEmpty()).forEach(entry -> {
      String[] split = entry.split(":");
      if (split.length != 2) {
        throw new IllegalArgumentException("Ungültiges Format: " + entry);
      }
      configMap.put(split[0].trim(), split[1].trim());
    });
    return processMap(configMap);
  }

  private SubjectNameStrategy createDefaultDelegate(String clazzName) {
    try {
      return createDelegate(clazzName);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  private SubjectNameStrategy createDelegate(String clazzName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    Class<?> clazz = Class.forName(clazzName);
    return (SubjectNameStrategy) clazz.getDeclaredConstructor().newInstance();
  }

  private Pattern obtainPattern(Object key) {
    if (key instanceof Pattern pattern) {
      return pattern;
    } else if (key instanceof String regex) {
      return Pattern.compile(regex.trim());
    } else {
      throw new IllegalArgumentException("Map Key muss ein Pattern oder String sein, kein " + key.getClass());
    }
  }

  private SubjectNameStrategy findDelegate(String topic) {
    // Check cache first
    var cached = delegateCache.get(topic);
    if (cached != null) {
      return cached;
    }
    // Compute and cache
    for (Entry<Pattern, SubjectNameStrategy> entry : delegates.entrySet()) {
      if (entry.getKey().matcher(topic).matches()) {
        delegateCache.put(topic, entry.getValue());
        return entry.getValue();
      }
    }
    if (defaultDelegate.get() == null) {
      throw new IllegalStateException("Keine SubjectNameStrategy für das Topic '" + topic + "' vorhanden.");
    }
    delegateCache.put(topic, defaultDelegate.get());
    return defaultDelegate.get();
  }
}