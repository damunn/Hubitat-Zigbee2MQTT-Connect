/**
 * ===============  Generic Component Acceleration/Axis/Contact Driver ===================
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License. You may obtain a copy of the License at:
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 *  for the specific language governing permissions and limitations under the License.
 *
 * =======================================================================================
 *
 *  Changelog:
 *	250823	1.0	Change driver name, namespace
 *  2021-11-13 - Initial release
 */

import groovy.transform.Field

@Field static final List<String> parsableAttributes = ["battery", "acceleration", "contact", "threeAxis", "temperature"]
@Field static final Integer debugAutoDisableMinutes = 30

metadata {
   definition(name: "zig2m Component Acceleration/Axis/Contact Sensor", namespace: "Zigbee2MQTT", author: "Robert Morris", component: true) {
      capability "Sensor"
      capability "Battery"
      capability "TemperatureMeasurement"
      capability "WaterSensor"
      capability "Refresh"
   }
   preferences {
      input name: "enableDebug", type: "bool", title: "Enable debug logging", defaultValue: true
      input name: "enableDesc", type: "bool", title: "Enable descriptionText logging", defaultValue: true
   }
}


#include Zigbee2MQTT.ComponentDriverLibrary_Common

// Using custom parse() here, NOT an include  for RMoRobert.zigbee2MQTTComponentDriverLibrary_Parse

void parse(String description) { log.warn "parse(String description) not implemented" }

void parse(List<Map> description) {
   if (enableDebug) log.debug ("parse($description)")
   description.each {
      if (it.name in parsableAttributes) {
         // these may come in one at a time, so piece together with current values if needeed
         if (it.name == "threeAxis") {
            Map<String,Number> currentAxes = device.currentValue("threeAxis") ?: [:]
            Map<String,Number> newAxes = (it.value != null) ? it.value : [:]
            if (!(newAxes.x)) newAxes.x = currentAxes.x ?: 0
            if (!(newAxes.y)) newAxes.y = currentAxes.y ?: 0
            if (!(newAxes.z)) newAxes.z = currentAxes.y ?: 0
            if (enableDesc && device.currentValue("threeAxis") != newAxes) {
               if (it.descriptionText != null) log.info it.descriptionText
               else log.info "${device.displayName} ${it.name} is ${newAxes}"
               sendEvent(name: "threeAxis", value: newAxes, descriptionText: "${device.displayName} threeAxis is ${newAxes}")
            }
         }
         else {
            if (enableDesc && device.currentValue(it.name) != it.value) {
               if (it.descriptionText != null) log.info it.descriptionText
               else log.info "${device.displayName} ${it.name} is ${it.value}"
            }
            sendEvent(it)
         }
      }
   }
}

