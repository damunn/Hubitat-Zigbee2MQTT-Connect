/**
 * ============================  Zigbee2MQTT Broker (Driver) =============================
 *
 *  Copyright 2021 Robert Morris
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
 *  Last modified: 2022-01-10
 *
 *  Changelog:
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
      namespace: "RMoRobert",
      author: "Robert Morris",
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
   runIn(4, "initialize")
}

void updated() {
   log.debug "updated()"
   initialize()
   state.parentId = parent.getTheAppId()
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
   runIn(4, "subscribeToTopic")
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
         DeviceWrapper hubitatDevice = parent.getChildDevice("Zig2M/${state.parentId ?: parent.getTheAppId()}/${ieeeAddress}")
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
      pauseExecution(250)
      subscribeToTopic()
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