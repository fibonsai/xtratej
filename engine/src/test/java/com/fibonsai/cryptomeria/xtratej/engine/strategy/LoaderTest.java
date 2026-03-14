/*
 *  Copyright (c) 2026 fibonsai.com
 *  All rights reserved.
 *
 *  This source is subject to the Apache License, Version 2.0.
 *  Please see the LICENSE file for more information.
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.fibonsai.cryptomeria.xtratej.engine.strategy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LoaderTest {

    private AutoCloseable closeable;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void finish() {
        try {
            closeable.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void fromJson_validJson_createsStrategies() {
        // Arrange
        String json = """
                {
                  "strategies": {
                    "testStrategy": {
                      "symbol": "TEST",
                      "type": "ENTER",
                      "sources": {
                        "flux1": {
                          "type": "SIMULATED",
                          "publisher": "test"
                        }
                      },
                      "rule": {
                        "type": "False"
                      }
                    }
                  }
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        assertNotNull(strategies);
        assertEquals(1, strategies.size());
        assertTrue(strategies.containsKey("testStrategy"));
        IStrategy strategy = strategies.get("testStrategy");
        assertEquals("testStrategy", strategy.name());
        assertEquals("TEST", strategy.symbol());
        assertEquals(IStrategy.StrategyType.ENTER, strategy.strategyType());
        assertNotNull(strategy.getSources());
        assertTrue(strategy.getSources().containsKey("flux1"));
    }

    @Test
    void fromJson_emptyStrategies_returnsEmpty() {
        String json = """
                {
                  "strategies": {}
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        assertNotNull(strategies);
        assertEquals(0, strategies.size());
    }

    @Test
    void fromJson_noStrategiesField_returnsEmpty() {
        String json = """
                {
                  "otherField": "value"
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        assertNotNull(strategies);
        assertEquals(0, strategies.size());
    }

    @Test
    void fromJson_missingFields() {
        String json = """
                {
                  "strategies": {
                    "minimalStrategy": {
                    }
                  }
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        assertNotNull(strategies);
        assertEquals(1, strategies.size());
    }

    @Test
    void fromJson_multipleStrategies_createsAll() {
        String json = """
                {
                  "strategies": {
                    "strategy1": {
                      "symbol": "S1",
                      "type": "ENTER",
                      "sources": {
                        "s1": {
                          "type": "SIMULATED",
                          "publisher": "pub1"
                        }
                      },
                      "rule": {
                        "type": "False"
                      }
                    },
                    "strategy2": {
                      "symbol": "S2",
                      "type": "EXIT",
                      "sources": {
                        "s2": {
                          "type": "SIMULATED",
                          "publisher": "pub2"
                        }
                      },
                      "rule": {
                        "type": "False"
                      }
                    }
                  }
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        assertEquals(2, strategies.size());
        assertTrue(strategies.containsKey("strategy1"));
        assertTrue(strategies.containsKey("strategy2"));
    }

    @Test
    void fromJson_withParams_passesToSubscriber() {
        String json = """
                {
                  "strategies": {
                    "strategyWithParams": {
                      "type": "ENTER",
                      "sources": {
                        "flux1": {
                          "type": "SIMULATED",
                          "publisher": "test",
                          "params": {
                            "interval": 1000,
                            "reconnect": true
                          }
                        }
                      },
                      "rule": {
                        "type": "False"
                      }
                    }
                  }
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        assertEquals(1, strategies.size());
        IStrategy strategy = strategies.get("strategyWithParams");
        assertNotNull(strategy.getSources().get("flux1"));
    }

    @Test
    void fromJson_nestedRuleStructure_parsesCorrectly() {
        String json = """
                {
                  "strategies": {
                    "nestedStrategy": {
                      "type": "ENTER",
                      "sources": {
                        "source1": {
                          "type": "SIMULATED",
                          "publisher": "test"
                        }
                      },
                      "rule": {
                        "type": "And",
                        "inputs": [
                          {
                            "type": "Limit",
                            "params": {
                              "min": 10,
                              "max": 20
                            }
                          },
                          {
                            "type": "Limit",
                            "params": {
                              "min": 5,
                              "max": 15
                            }
                          }
                        ]
                      }
                    }
                  }
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        assertEquals(1, strategies.size());
    }

    @Test
    void fromJson_withDescription_storesDescription() {
        String json = """
                {
                  "strategies": {
                    "strategyWithDesc": {
                      "type": "ENTER",
                      "description": "This is a test strategy",
                      "sources": {},
                      "rule": {
                        "type": "False"
                      }
                    }
                  }
                }
                """;

        Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));

        IStrategy strategy = strategies.get("strategyWithDesc");
        assertNotNull(strategy);
    }

    @Test
    void fromJson_invalidRuleType_logsError() {
        String json = """
                {
                  "strategies": {
                    "invalidRuleStrategy": {
                      "type": "ENTER",
                      "sources": {},
                      "rule": {
                        "type": "NonExistentRuleType"
                      }
                    }
                  }
                }
                """;

        assertDoesNotThrow(() -> {
            Map<String, IStrategy> strategies = Loader.fromJson(objectMapper.readValue(json, JsonNode.class));
            assertEquals(1, strategies.size());
        });
    }

    @Test
    void fromJson_sourceWithoutType_throwException() {
        String json = """
                {
                  "strategies": {
                    "sourceWithoutType": {
                      "type": "ENTER",
                      "sources": {
                        "flux1": {
                          "publisher": "test"
                        }
                      },
                      "rule": {
                        "type": "False"
                      }
                    }
                  }
                }
                """;

        assertThrows(RuntimeException.class, () -> {
            Loader.fromJson(objectMapper.readValue(json, JsonNode.class));
        });
    }

    @Test
    void fromJson_invalidJson_throwsException() {
        String invalidJson = """
                {
                  "strategies": {
                    "incomplete": {
                      "type": "ENTER"
                }
                """;

        assertThrows(Exception.class, () -> {
            Loader.fromJson(objectMapper.readTree(invalidJson));
        });
    }

    @Test
    void schemaKey_keysAreCorrect() {
        assertEquals("strategies", Loader.SchemaKey.STRATEGIES.key());
        assertEquals("symbol", Loader.SchemaKey.SYMBOL.key());
        assertEquals("type", Loader.SchemaKey.TYPE.key());
        assertEquals("sources", Loader.SchemaKey.SOURCES.key());
        assertEquals("publisher", Loader.SchemaKey.PUBLISHER.key());
        assertEquals("params", Loader.SchemaKey.PARAMS.key());
        assertEquals("rule", Loader.SchemaKey.RULE.key());
        assertEquals("inputs", Loader.SchemaKey.INPUTS.key());
        assertEquals("description", Loader.SchemaKey.DESCRIPTION.key());
    }

    @Test
    void emptyParams_isValidObjectNode() {
        JsonNode emptyParams = Loader.EMPTY_PARAMS;
        assertNotNull(emptyParams);
        assertTrue(emptyParams.isObject());
    }

    @Test
    void emptyArray_isValidArrayNode() {
        JsonNode emptyArray = Loader.EMPTY_ARRAY;
        assertNotNull(emptyArray);
        assertTrue(emptyArray.isArray());
        assertEquals(0, emptyArray.size());
    }
}
