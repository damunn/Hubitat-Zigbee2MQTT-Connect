/**
 * ============================  Zigbee2MQTT Button (Driver) =============================
 *
 *  Copyright 2025 Dale Munn /  Robert Morris
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
 *   1.0   250824 - Change name and namespace
 *  v0.4    - (Beta) Improved vendor/model to event matching; added more devices; improved battery parsing
 *  v0.3    - (Beta) Added battery parsing (if device supports)
 *  v0.2    - (Beta) Added additional vendor/model combinations for events
 *  v0.1    - (Beta) Initial Public Release
 */ 

import groovy.transform.Field
import java.util.concurrent.ConcurrentHashMap

// used for some "release" events (check last push/hold)
@Field static final ConcurrentHashMap<Long,Integer> lastButtonNumber = [:]
@Field static final Integer debugAutoDisableMinutes = 30

// [vendor: [model: ... ]]
@Field static final Map<String,Map<String,Map<String,Map<String,Integer>>>> buttonEventMap = [
   "AduroSmart": [
      "81825": [
         "on": [pushed: 1],
         "off": [pushed: 4],
         "up": [pushed: 2],
         "down": [pushed: 3]
      ]
   ],
   "IKEA": [
      "E1524/E1810": [ // 5-button "steering wheel"
         "toggle": [pushed: 1],
         "arrow_left_click": [pushed: 2],
         "arrow_left_hold": [held: 2],
         "arrow_left_release": [released: 2],
         "arrow_right_click": [pushed: 3],
         "arrow_right_hold": [held: 3],
         "arrow_right_release": [released: 3],
         "brightness_up_click": [pushed: 4],
         "brightness_up_hold": [held: 4],
         "brightness_up_release": [released: 4],
         "brightness_down_click": [pushed: 5],
         "brightness_down_hold": [held: 5],
         "brightness_down_release": [released: 5]
      ],
      "E2001/E2002": [ // STYRBAR remote control N2
         "toggle": [pushed: 1],
         "arrow_left_click": [pushed: 2],
         "arrow_left_hold": [held: 2],
         "arrow_left_release": [released: 2],
         "arrow_right_click": [pushed: 3],
         "arrow_right_hold": [held: 3],
         "arrow_right_release": [released: 3],
         "brightness_up_click": [pushed: 4],
         "brightness_up_hold": [held: 4],
         "brightness_up_release": [released: 4],
         "brightness_down_click": [pushed: 4],
         "brightness_down_hold": [held: 4],
         "brightness_down_release": [released: 4]
      ],
      "ICTC-G-1": [ // Tradfri wireless dimmer (round)
         "brightness_move_to_level": [pushed: 1],
         "brightness_move_up": [pushed: 2],
         "brightness_move_down": [pushed: 3],
         "brightness_stop": [released: 0]
      ],
      "E1743": [ // on/off switch
         "on": [pushed: 1],
         "off": [pushed: 2],
         "brightness_move_up": [held: 1],
         "brightness_move_down": [held: 2],
         "brightness_stop": [released: 0]
      ],
      "E1812": [ // shortcut button
         "on": [pushed: 1],
         "brightness_move_up": [held: 1],
         "brightness_stop": [released: 1],
      ],
      "E1766": [ // open/close remote (blinds?)
         "open": [pushed: 1],
         "close": [pushed: 2],
         "stop": [released: 0]
      ],
      "E1841": [ // open/close water remote
         "on": [pushed: 1],
         "off": [pushed: 2]
      ],
      "E1744": [ // Symfonisk sound controller
         "toggle": [pushed: 1],
         "brightness_step_up": [pushed: 2],
         "brightness_step_down": [pushed: 3],
         "brightness_move_up": [held: 2],
         "brightness_move_down": [held: 3],
         "brightness_stop": [released: 0]
      ]
   ],
   "Leedarson": [
      "6ARCZABZH": [  // EcoSmart 4-button remote, possibly others
         "on": [pushed: 1],
         "off": [pushed: 1],
         "brightness_up": [pushed: 2],
         "brightness_down": [pushed: 2],
         "colortemp_up": [pushed: 3],
         "colortemp_down": [pushed: 3],
         "colortemp_up_hold": [held: 3],
         "colortemp_down_hold": [held: 3],
         "colortemp_up_release": [released: 3],
         "colortemp_down_release": [released: 3],
         "recall": [pushed: 4]
      ]
   ],
   "Philips": [
      "324131092621": [ // Hue Dimmer v1
         "on-press": [pushed: 1],
         "on-hold": [held: 1],
         "on-hold-release": [released: 1],
         "up-press": [pushed: 2],
         "up-hold": [held: 2],
         "up-hold-release": [released: 2],
         "down-press": [pushed: 3],
         "down-hold": [held: 3],
         "down-hold-release": [released: 3],
         "off-press": [pushed: 4],
         "off-hold": [held: 4],
         "off-hold-release": [released: 4]
      ],
      "929002398602": [ // Hue Dimmer v2
         "on_press": [pushed: 1],
         "on_hold": [held: 1],
         "on_hold_release": [released: 1],
         "up_press": [pushed: 2],
         "up_hold": [held: 2],
         "up_hold_release": [released: 2],
         "down_press": [pushed: 3],
         "down_hold": [held: 3],
         "down_hold_release": [released: 3],
         "off_press": [pushed: 4],
         "off_hold": [held: 4],
         "off_hold_release": [released: 4]
      ],
      "8718696743133": [ // Hue Tap
         "press_1": [pushed: 1],
         "press_2": [pushed: 2],
         "press_3": [pushed: 3],
         "press_4": [pushed: 4]
      ],
      "8718699693985": [ // Hue Button
         //"on": [pushed: 1], // are these redundant with 'press'? Can change/uncomment if needed...
         //"off": [pushed: 1],
         "press": [pushed: 1],
         "hold": [held: 1],
         "release": [released: 1],
         "skip_backward": [pushed: 2],
         "skip_forward": [pushed: 3]
      ],
      "929003017102": [ // In-wall module
         "left_press": [pushed: 1],
         "left_press_release": [released: 1],
         "right_press": [pushed: 2],
         "right_press_release": [released: 2]
      ]
   ],
   "Sonoff": [
      "SNZB-01": [
         "single": [pushed: 1],
         "double": [pushed: 2],
         "long": [held: 1]
      ]
   ],
   "Xiaomi": [
      "WXCJKG11LM": [  // Opple 1-band
         "button_1_single": [pushed: 1],
         "button_1_hold": [held: 1],
         "button_1_release": [released: 1],
         "button_1_double": [pushed: 3],
         "button_1_triple": [pushed: 5],
         "button_2_single": [pushed: 2],
         "button_2_hold": [held: 2],
         "button_2_release": [released: 2],
         "button_2_double": [pushed: 4],
         "button_2_triple": [pushed: 6]
      ],
      "WXCJKG12LM": [  // Opple 2-band
         "button_1_single": [pushed: 1],
         "button_1_hold": [held: 1],
         "button_1_release": [released: 1],
         "button_1_double": [pushed: 5],
         "button_1_triple": [pushed: 9],
         "button_2_single": [pushed: 2],
         "button_2_hold": [held: 2],
         "button_2_release": [released: 2],
         "button_2_double": [pushed: 6],
         "button_2_triple": [pushed: 10],
         "button_3_single": [pushed: 3],
         "button_3_hold": [held: 3],
         "button_3_release": [released: 3],
         "button_3_double": [pushed: 7],
         "button_3_triple": [pushed: 11],
         "button_4_single": [pushed: 4],
         "button_4_hold": [held: 4],
         "button_4_release": [released: 4],
         "button_4_double": [pushed: 8],
         "button_4_triple": [pushed: 12]
      ],
      "WXCJKG13LM": [  // Opple 3-band
         "button_1_single": [pushed: 1],
         "button_1_hold": [held: 1],
         "button_1_release": [released: 1],
         "button_1_double": [pushed: 7],
         "button_1_triple": [pushed: 13],
         "button_2_single": [pushed: 2],
         "button_2_hold": [held: 2],
         "button_2_release": [released: 2],
         "button_2_double": [pushed: 8],
         "button_2_triple": [pushed: 14],
         "button_3_single": [pushed: 3],
         "button_3_hold": [held: 3],
         "button_3_release": [released: 3],
         "button_3_double": [pushed: 9],
         "button_3_triple": [pushed: 15],
         "button_4_single": [pushed: 4],
         "button_4_hold": [held: 4],
         "button_4_release": [released: 4],
         "button_4_double": [pushed: 10],
         "button_4_triple": [pushed: 16],
         "button_5_single": [pushed: 5],
         "button_5_hold": [held: 5],
         "button_5_release": [released: 5],
         "button_5_double": [pushed: 11],
         "button_5_triple": [pushed: 17],
         "button_6_single": [pushed: 6],
         "button_6_hold": [held: 6],
         "button_6_release": [released: 6],
         "button_6_double": [pushed: 12],
         "button_6_triple": [pushed: 18]
      ],
      "WXKG01LM": [  // MiJia wireless button (round)
         "single": [pushed: 1],
         "hold": [held: 1],
         "release": [released: 1],
         "double": [pushed: 2],
         "triple": [pushed: 3],
         "quadruple": [pushed: 4],
         "many": [pushed: 5]
      ],
      "WXKG02LM_rev1": [  // Aqara double wall switch (2016 model)
         "single_left": [pushed: 1],
         "single_right": [pushed: 2],
         "single_both": [pushed: 3]
      ],
      "WXKG02LM_rev2": [  // Aqara double wall switch (2018 model)
         "single_left": [pushed: 1],
         "single_right": [pushed: 2],
         "single_both": [pushed: 3],
         "double_left": [pushed: 4],
         "double_right": [pushed: 5],
         "double_both": [pushed: 6],
         "held_left": [held: 1],
         "held_right": [held: 2],
         "held_both": [held: 3]
      ],
      "WXKG03LM_rev1": [  // Aqara single wall switch (2016 model)
         "single": [pushed: 1]
      ],
      "WXKG03LM_rev2": [  // Aqara single wall switch (2018 model)
         "single": [pushed: 1],
         "double": [pushed: 2],
         "hold": [held: 1]
      ],
      "WXKG06LM": [  // Aqara D1 single wall switch
         "single": [pushed: 1],
         "double": [pushed: 2],
         "hold": [held: 1]
      ],
      "WXKG06LM": [  // Aqara D1 double wall switch
         "single_left": [pushed: 1],
         "single_right": [pushed: 2],
         "single_both": [pushed: 3],
         "double_left": [pushed: 4],
         "double_right": [pushed: 5],
         "double_both": [pushed: 6],
         "held_left": [held: 1],
         "held_right": [held: 2],
         "held_both": [held: 3]
      ],
      "WXKG11LM": [  // Aqara wireless button (square)
         "single": [pushed: 1],
         "hold": [held: 1],
         "release": [released: 1],
         "double": [pushed: 2],
         "triple": [pushed: 3],
         "quadruple": [pushed: 4]
      ],
      "WXKG12LM": [  // Aqara wireless button (square, with gyroscope)
         "single": [pushed: 1],
         "hold": [held: 1],
         "release": [released: 1],
         "double": [pushed: 2],
         "shake": [pushed: 3]
      ],
      "WXKG13LM": [  // Aqara T1 wireless button (square)
         "single": [pushed: 1],
         "hold": [held: 1],
         "release": [released: 1],
         "double": [pushed: 2],
         "triple": [pushed: 3],
         "quadruple": [pushed: 4],
         "quintuple": [pushed: 5],
         "many": [pushed: 5] // could make 6 if needed? do not have device to test if distinct...
      ]
   ]
]

metadata {
   definition (name: "zig2m Component Button", namespace: "Zigbee2MQTT", author: "Robert Morris",
               importUrl: "https://raw.githubusercontent.com/RMoRobert/HASSConnect/main/drivers/zig2m-button.groovy") {
      capability "Sensor"
      capability "Battery"
      capability "PushableButton"
      capability "HoldableButton"
      capability "ReleasableButton"

      command "setNumberOfButtons", ["NUMBER"]
      // Should not be needed during normal use:
      //command "setVendorAndModel". ["STRING", "STRING"]
   }
   
   preferences() {
      input name: "enableDebug", type: "bool", title: "Enable debug logging", defaultValue: true
      input name: "enableDesc", type: "bool", title: "Enable descriptionText logging", defaultValue: true
   }
}

#include Zigbee2MQTT.ComponentDriverLibrary_Common

// Using custom parse() here, NOT an include  for RMoRobert.zigbee2MQTTComponentDriverLibrary_Parse

void parse(description) {
   log.warn "not implemented: parse(Object $description)"
}

void parse(List<Map> data) {
   if (enableDebug) log.debug "parse(List $data)"
   Map rawBtnAction = data.find { it.name == "action" }
   if (rawBtnAction?.value) {
      Map<String,Integer> btnEvt = buttonEventMap.get(getDataValue("vendor"))?.get(getDataValue("model"))?.get(rawBtnAction.value)
      if (!btnEvt) {
         log.debug "no known button events; skipping"
      }
      else {
         String evtName = btnEvt.keySet()[0]
         Integer btnNum = btnEvt[btnEvt.keySet()[0]]
         if (btnNum == 0) {
            btnNum = lastButtonNumber[device.idAsLong] ?: 1
         }
         doSendButtonEvent(evtName, btnNum)
         lastButtonNumber[device.idAsLong] = btnNum
         evtName = null; btnNum = null
      }
   }
   else {
      if (data.find { it.name == "battery" }) {
         doSendEvent("battery", Math.round(data.find { it.name == "battery" }.value as float))
      }
   }
}

void setNumberOfButtons(Number number) {
   if (enableDebug) log.debug "setNumberOfButtons($number)"
   doSendEvent("numberOfButtons", number)
}

void push(Integer buttonNumber) {
   doSendEvent("pushed", buttonNumber)
}

void hold(Integer buttonNumber) {
   doSendEvent("held", buttonNumber)
}

void release(Integer buttonNumber) {
   doSendEvent("released", buttonNumber)
}

void setVendorAndModel(String vendor, String model) {
   device.updateDataValue("vendor", vendor)
   device.updateDataValue("model", model)
}

private void doSendButtonEvent(String eventName, Integer eventValue, Boolean forceStateChange = true) {
   //if (enableDebug) log.debug ("Creating event for $eventName...")
   String descriptionText = "${device.displayName} button ${eventValue} is ${eventName}"
   if (settings.enableDesc) log.info descriptionText
   sendEvent(name: eventName, value: eventValue, descriptionText: descriptionText, isStateChange: forceStateChange)
}

private void doSendEvent(String eventName, eventValue) {
   //if (enableDebug) log.debug "doSendEvent($eventName, $eventValue)"
   String descriptionText = "${device.displayName} ${eventName} is ${eventValue}"
   if (settings.enableDesc) log.info descriptionText
   sendEvent(name: eventName, value: eventValue, descriptionText: descriptionText)
}
