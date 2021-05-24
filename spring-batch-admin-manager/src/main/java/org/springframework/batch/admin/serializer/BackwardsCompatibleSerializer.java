package org.springframework.batch.admin.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.admin.web.JobController;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.dao.Jackson2ExecutionContextStringSerializer;
import org.springframework.lang.NonNull;
import org.springframework.util.ReflectionUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.springframework.util.Assert;

/**
 * Extends {@link Jackson2ExecutionContextStringSerializer} in order to support deserializing JSON
 * that was serialized using Spring Batch 4.2.1.RELEASE, and persisted in the database.
 *
 * <p>This class has been tested upgrading from Spring Batch 4.2.1.RELEASE to 4.2.4.RELEASE.
 */
public class BackwardsCompatibleSerializer extends Jackson2ExecutionContextStringSerializer {
private static Log log = LogFactory.getLog(BackwardsCompatibleSerializer.class);

  private final ObjectMapper newObjectMapper;

  private final ObjectMapper legacyObjectMapper;

  public BackwardsCompatibleSerializer() {
    newObjectMapper = getNewObjectMapper();
    legacyObjectMapper = createLegacyObjectMapper();
  }
  


  public void serialize(Map<String, Object> context, OutputStream out) throws IOException {

      Assert.notNull(context, "A context is required");
      Assert.notNull(out, "An OutputStream is required");
      //if(!context.isEmpty()) {
      legacyObjectMapper.writeValue(out, context);
      //}
      //super.serialize(context, out);
  }

  /**
   * Overrides the default deserialization method.  If an {@link InvalidTypeIdException} is thrown
   * during deserialization, the exception is caught, and an attempt is made to deserialize the JSON
   * using the legacy {@link ObjectMapper} instance.
   */
  @Override
  public @NotNull Map<String, Object> deserialize(@NotNull InputStream in) throws IOException {
	//log.info("deserialize");
    String json = inputStreamToString(in);
    
    TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};
    //log.info(json);
    try {
      return super.deserialize(in);
    } catch (Exception e) {
    	
      log.info("Couldn't deserialize JSON: will attempt to use legacy ObjectMapper");
      log.debug("Stacktrace", e);
      return legacyObjectMapper.readValue(json, typeRef);
    }
  }

  /**
   * Uses Java reflection to access the new {@link ObjectMapper} instance from the private
   * superclass field.  This will be used to serialize and deserialize JSON created using Spring
   * Batch 4.2.4.RELEASE.
   *
   * @return the new {@link ObjectMapper} instance
   */
  private ObjectMapper getNewObjectMapper() {
    ObjectMapper newObjectMapper;
    Field field = ReflectionUtils.findField(Jackson2ExecutionContextStringSerializer.class,
        "objectMapper", ObjectMapper.class);
    Objects.requireNonNull(field, "objectMapper field is null");
    ReflectionUtils.makeAccessible(field);
    newObjectMapper = (ObjectMapper) ReflectionUtils.getField(field, this);
    return newObjectMapper;
  }

  /**
   * Creates the {@link ObjectMapper} instance that can be used for deserializing JSON that was
   * previously serialized using Spring Batch 4.2.1.RELEASE.  This instance is only used if an
   * exception is thrown in {@link #deserialize(InputStream)} when using the new {@link
   * ObjectMapper} instance.
   *
   * @return the {@link ObjectMapper} instance that can be used for deserializing legacy JSON
   */
  @SuppressWarnings("deprecation")
  private ObjectMapper createLegacyObjectMapper() {
    ObjectMapper legacyObjectMapper = new ObjectMapper();
    legacyObjectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
    legacyObjectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    legacyObjectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    legacyObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
    //legacyObjectMapper.activateDefaultTyping(legacyObjectMapper.getPolymorphicTypeValidator());
    legacyObjectMapper.registerModule(new JobParametersModule());
    return legacyObjectMapper;
  }

  private static String inputStreamToString(@NonNull InputStream inputStream) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toString("UTF-8");
  }

  /*
   * The remainder of this file was copied from here:
   *
   * https://github.com/spring-projects/spring-batch/blob/4.2.1.RELEASE/spring-batch-core/src/main/java/org/springframework/batch/core/repository/dao/Jackson2ExecutionContextStringSerializer.java
   */

  // BATCH-2680

  /**
   * Custom Jackson module to support {@link JobParameter} and {@link JobParameters}
   * deserialization.
   */
  private static class JobParametersModule extends SimpleModule {

    private static final long serialVersionUID = 1L;

    private JobParametersModule() {
      super("Job parameters module");
      setMixInAnnotation(JobParameters.class, JobParametersMixIn.class);
      addDeserializer(JobParameter.class, new JobParameterDeserializer());
    }

    private abstract static class JobParametersMixIn {

      @JsonIgnore
      abstract boolean isEmpty();
    }

    private static class JobParameterDeserializer extends StdDeserializer<JobParameter> {

      private static final long serialVersionUID = 1L;
      private static final String IDENTIFYING_KEY_NAME = "identifying";
      private static final String TYPE_KEY_NAME = "type";
      private static final String VALUE_KEY_NAME = "value";

      JobParameterDeserializer() {
        super(JobParameter.class);
      }

      @SuppressWarnings("checkstyle:all")
      @Override
      public JobParameter deserialize(JsonParser parser, DeserializationContext context)
          throws IOException {
        JsonNode node = parser.readValueAsTree();
        boolean identifying = node.get(IDENTIFYING_KEY_NAME).asBoolean();
        String type = node.get(TYPE_KEY_NAME).asText();
        JsonNode value = node.get(VALUE_KEY_NAME);
        Object parameterValue;
        switch (JobParameter.ParameterType.valueOf(type)) {
          case STRING: {
            parameterValue = value.asText();
            return new JobParameter((String) parameterValue, identifying);
          }
          case DATE: {
            parameterValue = new Date(value.get(1).asLong());
            return new JobParameter((Date) parameterValue, identifying);
          }
          case LONG: {
            parameterValue = value.get(1).asLong();
            return new JobParameter((Long) parameterValue, identifying);
          }
          case DOUBLE: {
            parameterValue = value.asDouble();
            return new JobParameter((Double) parameterValue, identifying);
          }
        }
        return null;
      }
    }
  }
}