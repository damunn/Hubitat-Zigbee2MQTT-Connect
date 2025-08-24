/**
 * ================  Zigbee2MQTT Component Lock (with Codes) Driver ======================
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
 *  2021-12-12 - Initial release
 */

import groovy.transform.Field

@Field static final List<String> parsableAttributes = ["battery", "lock", "codeChanged", "codeLength",
   "lockCodes", "maxCodes"]
@Field static final Integer debugAutoDisableMinutes = 30

metadata {
   definition(name: "zig2m Component Lock (with Codes)", namespace: "Zigbee2MQTT", author: "Robert Morris", component: true) {
      capability "Sensor"
      capability "Battery"
      capability "TemperatureMeasurement"
      capability "RelativeHumidityMeasurement"
      capability "Refresh"
   }
   preferences {
      input name: "enableDebug", type: "bool", title: "Enable debug logging", defaultValue: true
      input name: "enableDesc", type: "bool", title: "Enable descriptionText logging", defaultValue: true
   }
}

void lock() {
   if (enableDebug) log.debug "lock()"
   parent.componentLock(this.device)
}

void unlock() {
   if (enableDebug) log.debug "unlock()"
   parent.componentUnlock(this.device)
}

void deleteCode(Integer codePosition) {
   if (enableDebug) log.debug "deleteCode($codePosition)"
   parent.componentDeleteCode(this.device)
   state.remove("codeName.${codePosition}")
}

void getCodes() {
   if (enableDebug) log.debug "getCodes()"
   parent.componentGetCodes(this.device)
}

void setCode(Integer codePosition, String pincode, String name=null) {
   if (enableDebug) log.debug "setCode($codePosition, $pincode, $name)"
   if (name) state["codeName.${codePosition}"] = name
   else state.remove("codeName.${codePosition}")
   parent.setCode(this.device, codePosition, pincode, name)
}

void setCodeLength(Integer pincodeLength) {
   if (enableDebug) log.debug "setCodeLength($pincodeLength)"
   sendEvent(name: "codeLength", value: pincodeLength, descriptionText: "${device.displayName} codeLength is ${pincodeLength}")
}

#include Zigbee2MQTT.ComponentDriverLibrary_Common

// Custom parse, so not including the parse library

void parse(String description) { log.warn "parse(String description) not implemented" }

void parse(List<Map> description) {
   if (enableDebug) log.debug ("parse($description)")
   description.each {
      if (it.name in parsableAttributes) {
         if (enableDesc && device.currentValue(it.name) != it.value) {
            if (it.descriptionText != null) log.info it.descriptionText
            else log.info "${device.displayName} ${it.name} is ${it.value}"
         }
         sendEvent(it)
      }
   }
}
