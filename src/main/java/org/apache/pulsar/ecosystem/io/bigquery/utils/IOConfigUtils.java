/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.bigquery.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.BaseContext;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Copy from {@link org.apache.pulsar.io.common.IOConfigUtils}, enhanced support required and defaultValue annotations.
 */
@Slf4j
public class IOConfigUtils {

    public static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz, BaseContext context) {
        return loadWithSecrets(map, clazz, secretName -> context.getSecret(secretName));
    }


    private static <T> T loadWithSecrets(Map<String, Object> map, Class<T> clazz,
                                         Function<String, String> secretsGetter) {
        Map<String, Object> configs = new HashMap<>(map);

        for (Field field : Reflections.getAllFields(clazz)) {
            field.setAccessible(true);
            for (Annotation annotation : field.getAnnotations()) {
                if (annotation.annotationType().equals(FieldDoc.class)) {
                    FieldDoc fieldDoc = (FieldDoc) annotation;
                    if (fieldDoc.sensitive()) {
                        String secret;
                        try {
                            secret = secretsGetter.apply(field.getName());
                        } catch (Exception e) {
                            log.warn("Failed to read secret {}", field.getName(), e);
                            break;
                        }
                        if (secret != null) {
                            configs.put(field.getName(), secret);
                        }
                    }
                    configs.computeIfAbsent(field.getName(), key -> {
                        if (fieldDoc.required()) {
                            throw new IllegalArgumentException(field.getName() + " cannot be null");
                        }
                        String value = fieldDoc.defaultValue();
                        if (!StringUtils.isEmpty(value)) {
                            return value;
                        }
                        return null;
                    });
                }
            }
        }

        List<String> fieldLists = Reflections.getAllFields(clazz).stream().map(field -> field.getName()).collect(
                Collectors.toList());
        Map<String, Object> finalConfigs =
                configs.entrySet().stream().filter(kv -> fieldLists.contains(kv.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new ObjectMapper().convertValue(finalConfigs, clazz);
    }
}
