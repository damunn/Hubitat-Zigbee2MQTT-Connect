/**
 * ============================  Zigbee2MQTT Broker (Driver) =============================
 *
 *  Copyright 2025 Dale Munn / Robert Morris
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
 *  Last modified: 2025-08-11
 *
 *  Changelog:
 *  v1.0    - 250811 Chage to child devuces under Broker
 *  v0.6    - (Beta) Improved reconnection attempt logic
 *  v0.5    - (Beta) Preliminary group suppport
 *  v0.4b   - (Beta) Added connection watchdog to broker (better reconnection after code updates, etc.)
 *  v0.4    - (Beta) Improved reconnection after code update, etc.
 *  v0.3    - (Beta) Added parsing to button events (most work done in button driver); improved driver/device matches
 *  v0.2    - (Beta) Improved reconnection; button driver and parsing added; more driver/device matches
 *  v0.1    - (Beta) Initial Public Release
 */ 

import groovy.json.JsonOutput
import groovy.transform.Field
import java.util.concurrent.ConcurrentHashMap
import com.hubitat.app.DeviceWrapper

// Automatically disable debug logging after (default=30)...
@Field static final Integer debugAutoDisableMinutes = 30
// Updating code disconnects MQTT without getting disconenction message; this is one way to handle that:
@Field static final ConcurrentHashMap<Long,Boolean> hasInitalized = [:]
@Field static final Boolean enableConnectionWatchdog = true
// Default "reconnect after..." seconds if MQTT status reports disconneted:
@Field static final Integer startingReconnectTime = 5
// List when received from Z2M:
@Field static final ConcurrentHashMap<Long,List> devices = [:]
@Field static final ConcurrentHashMap<Long,List> groups = [:]

metadata {
   definition(
      name: "Zigbee2MQTT Broker",
      namespace: "Zigbee2MQTT",
      author: "Robert Morris/Dale Munn",
      importUrl: "https://raw.githubusercontent.com/RMoRobert/Zigbee2MQTTDConnect/main/drivers/zig2m-broker.groovy"
   ) {
      capability "Actuator"
      capability "Initialize"

      command "connect"
      command "disconnect"

      // can remove when no longer under significant development; output devices and groups fields to logs
      command "logDevices"

      command "subscribeToTopic", [[name:"topic", type: "STRING", description: "MQTT topic"]]
      command "publish", [[name: "topic*", type: "STRING", description: "MQTT topic (will prepend with base topic)"],
                         [name: "payload", type: "STRING", description: "MQTT payload"]]

      attribute "status", "STRING"
   }
   
   preferences() {
      input name: "ipAddress", type: "string", title: "IP address", description: "Example: 192.168.0.10"
            required: true
      input name: "port", type: "number", title: "Port", description: "Default: 1883", defaultValue: 1883,
         required: true
      input name: "topic", type: "string", title: "MQTT topic", defaultValue: "zigbee2mqtt", required: true
      input name: "clientId", type: "string", title: "MQTT client ID", required: true
      //input name: "useTLS", type: "bool", title: "Use TLS/SSL", submitOnChange: true
      input name: "username", type: "string", title: "MQTT username (optional)", submitOnChange: true
      input name: "password", type: "password", title: "MQTT password (optional)", submitOnChange: true
      input name: "enableDebug", type: "bool", title: "Enable debug logging", defaultValue: true
      input name: "enableDesc", type: "bool", title: "Enable descriptionText logging", defaultValue: true
   }
}

void installed() {
   log.debug "installed()"
   state.parentId = parent.getTheAppId()
   runIn(4, "initialize")
}

void updated() {
   log.debug "updated()"
	return
   initialize()
   // TODO: See if this works or is needed:
   List<Map<String,Map>> newSettings = []
   newSettings << ["ipAddress": [value: settings.ipAddress, type: "string"]]
   newSettings << ["port": [value: settings.port, type: "number"]]
   newSettings << ["topic": [value: settings.topic, type: "string"]]
   newSettings << ["clientId": [value: settings.clientId, type: "string"]]
   newSettings << ["": [value: settings.useTLS, type: "bool"]]
   newSettings << ["username": [value: settings.username, type: "string"]]
   newSettings << ["password": [value: settings.password, type: "password"]]
   parent.updateSettings(newSettings)
}

void initialize(Boolean forceReconnect=true) {
   log.debug "initialize()"
   // This doesn't work...maybe working from cache? trying to give time to "commit" settings after initial device
   // install to avoid error. Is easily worked around by runIn() in installed() method, but could be cleaner...
   if (hasInitalized[device.idAsLong]) pauseExecution(2000)
   if (enableDebug) {
      log.debug "Debug logging will be automatically disabled in ${debugAutoDisableMinutes} minutes"
      runIn(debugAutoDisableMinutes*60, "debugOff")
   }
   if (forceReconnect == true) {
      doSendEvent("status", "disconnected")
      pauseExecution(750)
      reconnect(false)
   }
   else {
      reconnect()
   }
   if (enableConnectionWatchdog == true) {
      runEvery1Minute("connectionWatchdog")
   }
   else {
      unschedule("connectionWatchdog")
   }
}

void debugOff() {
   log.warn "Disabling debug logging"
   device.updateSetting("enableDebug", [value:"false", type:"bool"])
}

// Pieces together connection informatino into URI
String getConnectionUri() {
   //String uri = (settings.useTLS ? "ssl://" : "tcp://") + settings.ipAddress + ":" + settings.port
   String uri = "tcp://" + settings.ipAddress + ":" + settings.port
   return uri
}

void connect() {
   if (enableDebug) {
      log.debug "connect()"
      log.debug "URI = ${getConnectionUri()}; clientId = $settings.clientId; username = $username ($password)"
   }
   if (interfaces.mqtt.isConnected()) {
      if (enableDebug) log.debug "Is connected; disconnecting first"
      interfaces.mqtt.disconnect()
   }
   unschedule("reconnect")
   if (enableDebug) log.debug "connecting now..."
   interfaces.mqtt.connect(getConnectionUri(), settings.clientId, settings.username, settings.password)
   pauseExecution(1000)
//   runIn(4, "subscribeToTopic")
}

void updateSettings(List<Map<String,Map>> newSettings) {
   if (enableDebug) log.debug "updateSettings($newSettings)"
   newSettings.each { Map newSetting ->
      newSetting.each { String settingName, Map settingValue ->
         device.updateSetting(settingName, settingValue)
      }
   }
   initialize(true)
}

void disconnect() {
   if (enableDebug) log.debug "disconnect()"
   interfaces.mqtt.disconnect()
   unschedule("reconnect")
   doSendEvent("status", "disconnected")
}

void reconnect(Boolean notIfAlreadyConnected = true) {
   if (enableDebug) log.debug "reconnect(notIfAlreadyConnected=$notIfAlreadyConnected)"
   if (interfaces.mqtt.isConnected() && notIfAlreadyConnected) {
      if (enableDebug) log.debug "already connected; skipping reconnection"
   }
   else {
      connect()
   }
}

// Can be run periodically to check if "initialized," i.e., if code was updated and MQTT was
// disconnected without an event to let the driver know
// Long-term: consider adding MQTT and/or Z2M health check
void connectionWatchdog() {
   if (hasInitalized[device.idAsLong] != true) {
      initialize()
      hasInitalized[device.idAsLong] = true
   }
   else if (!(interfaces.mqtt.isConnected())) {
      doSendEvent("status", "disconnected")
      // startingReconnectTime is default if previously connected successfully, so this should only happen the first time
      // rather than doing this more often if the watchdog runs more often than the reconect attempt:
      if (state.connectionRetryTime == startingReconnectTime) runIn(state.connectionRetryTime, "reconnect")
   }
   else if (device.currentValue("disconnected")) {
      if (state.connectionRetryTime == startingReconnectTime) runIn(state.connectionRetryTime, "reconnect")
   }
}

void parse(String message) {
   if (enableDebug) log.debug "parse(): ${interfaces.mqtt.parseMessage(message)}"
   // Use if need to see raw data instead:
   //if (enableDebug) log.debug "parse(): raw message = $message"
   Map<String,String> parsedMsg = interfaces.mqtt.parseMessage(message)
   switch (parsedMsg?.topic) {
      // Should happen when subscribing or when device added, friendly name re-named, etc.:
      case "${settings.topic}/bridge/devices":
         devices[device.idAsLong] = parseJson(parsedMsg.payload)
         //log.trace "devices = ${devices[device.idAsLong]}"
         break
      case { it.startsWith("${settings.topic}/bridge/groups") }:
         groups[device.idAsLong] = parseJson(parsedMsg.payload)
         break
      // General Bridge info, some of which we may care about but are ignoring for now...but helps filter remaining to just devices
      case { it.startsWith("${settings.topic}/bridge/") }:
         if (enableDebug) log.debug "ignoring bridge topic ${parsedMsg.topic}, payload ${parsedMsg.payload}"
         break
      // Legacy button, ignore (use new)
      case { it.startsWith("${settings.topic}/") && (it.tokenize('/')[-1] == "click") && it.count('/') > 1 }:
         if (enableDebug) log.debug "ignoring /click (legacy button; use {'action': ...} instead)"
         break
      // Some "new" buttons seem to also do this instead of just the {"action": ...} payload, so ignore...
      case { it.startsWith("${settings.topic}/") && (it.tokenize('/')[-1] == "action") && it.count('/') > 1 }:
         if (enableDebug) log.debug "ignoring /action (will use payload {'action': ...} instead)"
         break
      // Not sure if this ever gets *recevied* or just sent, but just in case...
      case { it.startsWith("${settings.topic}/") && (it.tokenize('/')[-1] == "get") }:
         if (enableDebug) log.debug "ignoring /get ${parsedMsg.topic}, payload ${parsedMsg.payload}"
         break
      // Not sure if this (also) ever gets recevied or just sent, but just in case...
      case { it.startsWith("${settings.topic}/") && (it.tokenize('/')[-1] == "set") }:
         if (enableDebug) log.debug "ignoring /set ${parsedMsg.topic}, payload ${parsedMsg.payload}"
         break
      // Not using now, possibly could some day..
      case { it.startsWith("${settings.topic}/") && (it.tokenize('/')[-1] == "availability") }:
         if (enableDebug) log.debug "ignoring /availability ${parsedMsg.topic}, payload ${parsedMsg.payload}"
         break
      case { it.startsWith("${settings.topic}/") && (it.indexOf('/', "${settings.topic}/".size()) < 0) }:
         //log.trace "is device --> ${parsedMsg.topic} ---> PAYLOAD: ${parsedMsg.payload}"
         String friendlyName = parsedMsg.topic.substring("${settings.topic}/".length())
         String ieeeAddress =  devices[device.idAsLong].find { it.friendly_name == friendlyName }.ieee_address
         DeviceWrapper hubitatDevice = getChildDevice("Zig2M/${state.parentId ?: parent.getTheAppId()}/${ieeeAddress}")
         if (hubitatDevice == null) break
         //log.trace "**DEV = $hubitatDevice"
         List<Map> evts = parsePayloadToEvents(friendlyName, parsedMsg.payload)
         if (evts) hubitatDevice.parse(evts)
         break
      default:
         if (enableDebug) log.debug "ignore: $parsedMsg"
   }
}

List<Map> parsePayloadToEvents(String friendlyName, String payload) {
   if (enableDebug) log.debug "parsePayloadToEvents($friendlyName, $payload)"
      List<Map> eventList = []
   if (payload.startsWith("{") || payload.trim().startsWith("{")) {
      Map payloadMap = parseJson(payload)
      //log.warn payloadMap
      String colorMode = payloadMap.color_mode
      payloadMap.each { String key, value ->
         switch (key) {
            ///// Actuators
            case "state":
               String eventValue = (value == "ON") ? "on" : "off"
               eventList << [name: "switch", value: eventValue]
               break
            case "brightness":
               if (value == null) break
               Integer eventValue = Math.round((value as Float) / 255 * 100)
               eventList << [name: "level", value: eventValue, unit: "%"] 
               break
            case "color_temp":
            if (value == null) break
               Integer eventValue = Math.round(1000000.0 / (value as Float))
               eventList << [name: "colorTemperature", value: eventValue, unit: "K"]
               if (colorMode == "ct") {
                  eventList << getGenericColorTempName(eventValue)
                  eventList << ["colorMode": "CT"]
               }
               break
            // TODO: light effects
            case "color":
               Map<String,Integer> parsedHS = [:]
               if (value.hue != null) {
                  Integer eventValue = Math.round((value.hue as Float) / 3.6)
                  eventList << [name: "hue", value: eventValue, unit: "%"]
                  parsedHS['hue'] = eventValue
               }
               if (value.saturation != null) {
                  Integer eventValue = value.saturation
                  eventList << [name: "saturation", value: eventValue, unit: "%"]
                  parsedHS['saturation'] = eventValue 
               }
               if (!parsedHS) {
                  if (enableDebug) log.debug "not parsing color because hue/sat not provided (may be xy-only?)"
               }
               else {
                  if (colorMode != "ct") {
                     eventList << getGenericColorName(parsedHS.hue, parsedHS.saturation)
                     eventList << ["colorMode": "RGB"]
                  }
               }
               break
            ///// Sensors
            case "battery":
               if (value == null || !value) break
               Integer eventValue = Math.round(value as float)
               eventList << [name: "battery", value: eventValue] 
               break
            case "contact":
               String eventValue = value == true ? "closed" : "open"
               eventList << [name: "contact", value: eventValue] 
               break
            case "humidity":
               if (value == null) break
               Integer eventValue = Math.round(value as Float)
               eventList << [name: "humidity", value: eventValue, unit: "%"] 
               break
            case "illuminance_lux":
               if (value == null) break
               Integer eventValue = Math.round(value as Float)
               eventList << [name: "illuminance", value: eventValue, unit: "lux"] 
               break
            case "moving":
               String eventValue = value == true ? "active" : "inactive"
               eventList << [name: "acceleration", value: eventValue] 
               break
            case "occupancy":
               String eventValue = value == true ? "active" : "inactive"
               eventList << [name: "motion", value: eventValue] 
               break
            case "temperature":
               if (value == null) break
               String origUnit = (devices[device.idAsLong].find { friendly_name == friendlyName }?.definition?.exposes?.find {
                     it.name == "contact"
                  }?.unit?.endsWith("F")) ? "F" : "C"
               BigDecimal eventValue
               if ((origUnit == "C" && location.temperatureScale == "C") || (origUnit == "F" && location.temperatureScale == "F")) {
                  eventValue = ((value as BigDecimal)).setScale(1, java.math.RoundingMode.HALF_UP)
               }
               else if (origUnit == "C" && location.temperatureScale == "F") {
                  eventValue = celsiusToFahrenheit((value as BigDecimal)).setScale(1, java.math.RoundingMode.HALF_UP)
               }
               else { // origUnit == "F" && location.temperatureScale == "C" 
                  eventValue = fahrenheitTocelsius((value as BigDecimal)).setScale(1, java.math.RoundingMode.HALF_UP)
               }
               eventList << [name: "temperature", value: eventValue, unit: "°${location.temperatureScale}"] 
               break
            case "water_leak":
               String eventValue = value == true ? "wet" : "dry"
               eventList << [name: "water", value: eventValue] 
               break
            case { it.endsWith("_axis") && it.length() == 6 }:
               eventList << [name: "threeAxis", value: [(key.getAt(0).toLowerCase()): value]]
               break
            ///// Buttons
            case "action":
               // a bit different from the rest; gets converted to Hubitat-friendly events in custom driver:
               eventList << [name: "action", value: value]
               break
            case "linkquality":
            case "update":
               break
            default:
               if (enableDebug) log.debug "ignoring $key = $value"
         }
      }
   }
   else {
      if (enableDebug) log.debug "not parsing payload to events because probably not JSON: $payload"
   }
   eventList.each {
      if (it.descriptionText == null) {
         it.descriptionText = "${friendlyName} ${it.name} is ${it.value}"
      }
   }
   if (enableDebug) log.debug "eventList = $eventList"
   return eventList
}

void mqttClientStatus(String message) {
   if (enableDebug) log.debug "mqttClientStatus($message)"
   if ((message.startsWith("Status: Connection succeeded"))) {
      doSendEvent("status", "connected")
      state.connectionRetryTime = startingReconnectTime
      unschedule("reconnect")
      runIn(4, "subscribeToTopic")
  // pauseExecution(250)
  //    subscribeToTopic()
   }
   else if (!(interfaces.mqtt.isConnected())) {
      doSendEvent("status", "disconnected")
      if (!(state.connectionRetryTime)) {
         state.connectionRetryTime = startingReconnectTime
      }
      else if (state.connectionRetryTime < 60) {
         state.connectionRetryTime += 10
      }
      else if (state.connectionRetryTime < 300) {
         state.connectionRetryTime += 30
      }
      else {
         state.connectionRetryTime = 300 // cap at 5 minutes
      }
      runIn(state.connectionRetryTime, "reconnect")
   }
   else {
      log.warn "MQTT client status: $message"
   }
}

void subscribeToTopic(String toTopic = "${settings.topic}/#") {
   if (enableDebug) log.debug "subscribe($toTopic)"
   //if (enableDebug) log.debug "is connected = ${interfaces.mqtt.isConnected()}"
   interfaces.mqtt.subscribe(toTopic)
}

// Note: prepends base topic to 'topic' parameter
void publish(String topic, String payload="", Integer qos=0, Boolean retained=false) {
   if (enableDebug) log.debug "publish(topic = $topic, payload = $payload, qos = $qos, retained = $retained)"
   interfaces.mqtt.publish("${settings.topic}/${topic}", payload, qos, retained)
}

// Finds device IEEE and prepends base topic and friendly name to 'topic' parameter
void publishForIEEE(String ieee, String topic=null, Object jsonPayload="", Integer qos=0, Boolean retained=false) {
   if (enableDebug) log.debug "publishForIEEE(ieee = $ieee, topic = $topic, jsonPayload = $jsonPayload, qos = $qos, retained = $retained)"
   String friendlyName = devices[device.idAsLong].find { it.ieee_address == ieee }?.friendly_name
   if (friendlyName != null) {
      String fullTopic = topic ? "${settings.topic}/${friendlyName}/${topic}" : "${settings.topic}/${friendlyName}"
      String stringPayload
      if (jsonPayload instanceof String || jsonPayload instanceof GString) {
         stringPayload = jsonPayload
      }
      else {
         stringPayload = JsonOutput.toJson(jsonPayload)
      }
      if (enableDebug) log.debug "publishing: topic = ${fullTopic}, payload = ${stringPayload}"
      interfaces.mqtt.publish(fullTopic, stringPayload, qos, retained)
   }
   else {
      if (enableDebug) log.debug "not publishing; no device found for IEEE $ieee"
   }
}

// Returns devices in "raw" Z2M format (except parsed into List); used by parent app
List getDeviceList() {
   return devices[device.idAsLong] ?: []
}

// Returns devices in "raw" Z2M format (except parsed into List); used by parent app
List getGroupList() {
   return groups[device.idAsLong] ?: []
}

void logDevices(Boolean prettyPrint=true) {
   if (!prettyPrint) {
      log.trace devices[device.idAsLong]
      if (includeGroups) log.trace groups[device.idAsLong]
   }
   else {
      log.trace groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(devices[device.idAsLong]))
      if (includeGroups) log.trace groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(groups[device.idAsLong]))
   }
}

DeviceWrapper brokerChildDevice(devDNI){
   return getChildDevice(devDNI)
}

DeviceWrapper brokerAddChildDevice(String namespace, String typeName, String deviceNetworkId, Map properties = [:]) {
   return addChildDevice(namespace, typeName, deviceNetworkId, properties )
}

List brokerGetChildDevices() {
   return getChildDevices()
}

// Hubiat-provided color/name mappings
Map<String,String> getGenericColorName(Number hue, Number saturation=100, Boolean hiRezHue=false) {
   String colorName
   hue = hue.toInteger()
   if (!hiRezHue) hue = (hue * 3.6)
   switch (hue.toInteger()) {
      case 0..15: colorName = "Red"
         break
      case 16..45: colorName = "Orange"
         break
      case 46..75: colorName = "Yellow"
         break
      case 76..105: colorName = "Chartreuse"
         break
      case 106..135: colorName = "Green"
         break
      case 136..165: colorName = "Spring"
         break
      case 166..195: colorName = "Cyan"
         break
      case 196..225: colorName = "Azure"
         break
      case 226..255: colorName = "Blue"
         break
      case 256..285: colorName = "Violet"
         break
      case 286..315: colorName = "Magenta"
         break
      case 316..345: colorName = "Rose"
         break
      case 346..360: colorName = "Red"
         break
      default: colorName = "undefined" // shouldn't happen, but just in case
         break
   }
   if (saturation < 1) colorName = "White"
   return [name: "colorName", value: colorName]
}

// Hubitat-provided ct/name mappings
Map<String,String> getGenericColorTempName(Number temp) {
   if (!temp) return
   String genericName
   Integer value = temp.toInteger()
   if (value <= 2000) genericName = "Sodium"
   else if (value <= 2100) genericName = "Starlight"
   else if (value < 2400) genericName = "Sunrise"
   else if (value < 2800) genericName = "Incandescent"
   else if (value < 3300) genericName = "Soft White"
   else if (value < 3500) genericName = "Warm White"
   else if (value < 4150) genericName = "Moonlight"
   else if (value <= 5000) genericName = "Horizon"
   else if (value < 5500) genericName = "Daylight"
   else if (value < 6000) genericName = "Electronic"
   else if (value <= 6500) genericName = "Skylight"
   else if (value < 20000) genericName = "Polar"
   else genericName = "undefined" // shouldn't happen, but just in case
   return [name: "colorName", value: genericName]
}

private void doSendEvent(String eventName, eventValue) {
   //if (enableDebug) log.debug ("Creating event for $eventName...")
   String descriptionText = "${device.displayName} ${eventName} is ${eventValue}"
   if (settings.enableDesc) log.info descriptionText
   sendEvent(name: eventName, value: eventValue, descriptionText: descriptionText)
}

////////////////////////////////////
// Component Methods
////////////////////////////////////

void componentRefresh(DeviceWrapper device, List<Map<String,String>> payloads) {
   if (enableDebug) log.debug "componentRefresh(${device.displayName}), $payloads"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   payloads.each { Map<String,String> payload ->
      publishForIEEE(ieee, "get", payload)
      pauseExecution(50)
   }
}

void componentRefresh(DeviceWrapper device) {
   if (enableDebug) log.debug "componentRefresh(${device.displayName})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   List<Map<String,String>> payloads = []
   if (device.hasAttribute("switch")) payloads << [state: ""]
   if (device.hasAttribute("level")) payloads << [brightness: ""]
   if (device.hasAttribute("hue")) payloads << [color: [x: "", y: ""]]
   if (device.hasAttribute("colorTemperature")) payloads << [color_temp: ""]
   if (device.hasAttribute("lock")) payloads << [state: ""]
   // probably can flesh this out more for other devices later...
   payloads.each { Map<String,String> payload ->
      publishForIEEE(ieee, "get", payload)
      pauseExecution(50)
   }
}

void componentOn(DeviceWrapper device) {
   if (enableDebug) log.debug "componentOn(${device.displayName})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,String> payload = [state: "ON"]
   publishForIEEE(ieee, "set", payload)
}

void componentOff(DeviceWrapper device) {
   if (enableDebug) log.debug "componentOn(${device.displayName})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,String> payload = [state: "OFF"]
   publishForIEEE(ieee, "set", payload)
}

void componentSetLevel(DeviceWrapper device, Number level, Number transitionTime=null) {
   if (enableDebug) log.debug "componentSetLevel(${device.displayName}, $level, $transitionTime)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,Number> payload = [brightness: Math.round((level as Float) * 2.55)]
   if (transitionTime != null) payload << [transition: transitionTime]
   publishForIEEE(ieee, "set", payload)
}

/* Haven't found Z2M bulb that supports yet...
void componentPresetLevel(DeviceWrapper device, Number level) {
   if (enableDebug) log.debug "componentPresetLevel(${device.displayName}, $level)"
   DeviceWrapper brokerDev = getChildDevice("Zig2M/${app.id}")
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,Number> payload = [brightness: Math.round((level as Float) * 2.55)]
   brokerDev.publishForIEEE(ieee, "set", payload)
}
*/

void componentStartLevelChange(DeviceWrapper device, String direction) {
   if (enableDebug) log.debug "componentStartLevelChange(${device.displayName}, $direction)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Integer mvRate = (direction.toLowerCase() == "up" ? 85 : -85)
   Map<String,Number> payload = [brightness_move: mvRate]
   publishForIEEE(ieee, "set", payload)
}

void componentStopLevelChange(DeviceWrapper device) {
   if (enableDebug) log.debug "componentStopLevelChange(${device.displayName})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,Number> payload = [brightness_move: 0]
   publishForIEEE(ieee, "set", payload)
}

void componentSetColorTemperature(DeviceWrapper device, Number colorTemperature, Number level=null, Number transitionTime=null) {
   if (enableDebug) log.debug "componentSetColorTemperature(${device.displayName}, $colorTemperature, $level, $transitionTime)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,Number> payload = [color_temp: Math.round(1000000.0/colorTemperature)]
   if (level != null) payload << [brightness: Math.round((level as Float) * 2.55)]
   if (transitionTime != null) payload << [transition: transitionTime]
   if (device.currentValue("switch") != "on") payload << [state: "ON"]
   publishForIEEE(ieee, "set", payload)
}

// Uses RGB (most widely accepted?)
void componentSetColor(DeviceWrapper device, Map<String,Number> colorMap) {
   if (enableDebug) log.debug "componentSetColor(${device.displayName}, $colorMap)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   List rgb = hubitat.helper.ColorUtils.hsvToRGB([colorMap.hue, colorMap.saturation, colorMap.level ?: device.currentValue('level')])
   Map<String,Map<String,String>> payload = [color: [rgb: rgb.join(',')]]
   if (colorMap.rate != null) payload << [transition: colorMap.rate]
   if (device.currentValue("switch") != "on") payload << [state: "ON"]
   publishForIEEE(ieee, "set", payload)
}

// Uses HS (doesn't work for all?)
void componentSetColorHS(DeviceWrapper device, Map<String,Number> colorMap) {
   if (enableDebug) log.debug "componentSetColor(${device.displayName}, $colorMap)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,Map<String,String>> payload = [color: [h: Math.round((colorMap.hue as float) / 3.60), s: colorMap.saturation, v: colorMap.level ?: device.currentValue('level')]]
   if (colorMap.rate != null) payload << [transition: colorMap.rate]
   if (device.currentValue("switch") != "on") payload << [state: "ON"]
   publishForIEEE(ieee, "set", payload)
}

void componentSetHue(DeviceWrapper device, Number hue) {
   if (enableDebug) log.debug "componentSetHue(${device.displayName}, $hue)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   List rgb = hubitat.helper.ColorUtils.hsvToRGB([hue, device.currentValue('saturation'), device.currentValue('level')])
   Map<String,Map<String,String>> payload = [color: [rgb: rgb.join(',')]]
   if (device.currentValue("switch") != "on") payload << [state: "ON"]
   publishForIEEE(ieee, "set", payload)
}

void componentSetSaturation(DeviceWrapper device, Number sat) {
   if (enableDebug) log.debug "componentSetSaturation(${device.displayName}, $sat)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   List rgb = hubitat.helper.ColorUtils.hsvToRGB([device.currentValue('hue'), sat, device.currentValue('level')])
   Map<String,Map<String,String>> payload = [color: [rgb: rgb.join(',')]]
   if (device.currentValue("switch") != "on") payload << [state: "ON"]
   publishForIEEE(ieee, "set", payload)
}

// Hubitat uses number, but String is easier to work with in Z2M, so custom
// bulb driver implements both. Use the String variant only when calling parent for now!
void componentSetEffect(DeviceWrapper device, String effectName) {
   if (enableDebug) log.debug "componentSetEffect(${device.displayName}, String $effectName)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,Number> payload = [effect: effectName]
   publishForIEEE(ieee, "set", payload)
}

void componentSetEffect(DeviceWrapper device, Number effectNumber) {
   if (enableDebug) log.debug "componentSetEffect(${device.displayName}, Number $effectNumber)"
   log.warn "Not yet implemented; use String effecet name instead of number for now."
}
 
void componentPublish(DeviceWrapper device, String topic=null, String payload=null) {
   if (enableDebug) log.debug "componentPublish(${device.displayName}, $topic, $payload)"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   publishForIEEE(ieee, topic, payload)
} 

void componentLock(DeviceWrapper device) {
   if (enableDebug) log.debug "componentLock(${device.displayName})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,String> payload = [state: "LOCK"]
   publishForIEEE(ieee, "set", payload)
}

void componentUnlock(DeviceWrapper device) {
   if (enableDebug) log.debug "componentUnlock(${device.displayName})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,String> payload = [state: "UNLOCK"]
   publishForIEEE(ieee, "set", payload)
}

void componentDeleteCode(DeviceWrapper device, Integer codePosition) {
   if (enableDebug) log.debug "componentDeleteCode(${device.displayName}, ${codePosition})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,String> payload = [pin_code: [user: codePosition, user_enabled: false, pin_code: null]]
   publishForIEEE(ieee, "set", payload)
}

void componentGetCodes(DeviceWrapper device) {
   if (enableDebug) log.debug "componentDeleteCode(${device.displayName}, ${codePosition})"
   log.warn "getCodes() not implemented"
}

void componentSetCode(DeviceWrapper device, Integer codePosition, String pincode, String name=null) {
   if (enableDebug) log.debug "componentSetCode(${device.displayName}, ${codePosition})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
   Map<String,String> payload = [pin_code: [user: codePosition, user_enabled: true, pin_code: Integer.parseInt(pincode)]]
   publishForIEEE(ieee, "set", payload)
}

void componentSetCodeLength(DeviceWrapper device, Integer codeLength) {
   if (enableDebug) log.debug "componentSetCodeLength(${device.displayName}, ${codeLength})"
   List<Map> evts = [[name: "codeLength", value: codeLength]]
   device.parse(evts)
}

String getDefinitionForDevice(DeviceWrapper device) {
   if (enableDebug) log.debug "getDefinitionForDevice(${device.displayName})"
   String ieee = device.getDeviceNetworkId().tokenize('/')[-1]
      List zigDevs = getDeviceList()
      Map z2mDev = zigDevs.find { it.ieee_address == ieee }
      if (z2mDev != null) {
         // Can use if want Groovy toString output instead:
         //log.debug "DEFINITION: ${z2mDev.definition}"
         log.debug "DEFINITION: " + groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(z2mDev.definition))
      }
      else {
         log.debug "No device found on Zigbee2MQTT broker for ${device.displayName}"
      }
   return 
}
