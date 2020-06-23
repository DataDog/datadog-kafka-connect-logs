/**
 * This attribute is used during documentation generation to write the introduction section.
 */
@Introduction("This plugin is used to export Kafka records as logs to Datadog.")
/**
 * This attribute is used as the display name during documentation generation.
 */
@Title("kafka-connect-datadog-logs")
/**
 * This attribute is used to provide the owner on the connect hub. For example jcustenborder.
 */
@PluginOwner("com.datadoghq")
/**
 * This attribute is used to provide the name of the plugin on the connect hub.
 */
@PluginName("kafka-connect-datadog-logs")
package com.datadoghq;

import com.github.jcustenborder.kafka.connect.utils.config.Introduction;
import com.github.jcustenborder.kafka.connect.utils.config.PluginName;
import com.github.jcustenborder.kafka.connect.utils.config.PluginOwner;
import com.github.jcustenborder.kafka.connect.utils.config.Title;