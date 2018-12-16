package com.test.mina.server.utils

// 定义与华辰智通通信用的结构体

// 心跳请求与回复结构帧
case class HeartBeatFrame(gid: String, ptid: Int, cid: Int, var time: String, var func: Int)
case class RealTimeDataFrame(gid: String, ptid: Int, cid: Int, time: String, func: Int,
                             level: Int, consume: Int, err: Int, point: Any)

case class RealTimeCtrlDataFrame(gid: String, ptid: Int, cid: Int, time: String, func: Int,
                                 level: Int, consume: Int, err: Int, info: String)

case class SyncTimeDataFrame(gid: String, ptid: Int, cid: Int, time: String, func: Int, server_datetime: String)

case class Agps(Time: String, ICCID: String, IMEI: String, RSSI: Int, AccessServer: String,
                Signal: Int, MCC: Int, MNC: Int, CI: Long, LAC: Long)

case class AgpsDataFrame(gid: String, ptid: Int, cid: Int, time: String, func: Int, agps: Agps)

case class RemoteCtrlReqDataFrame(gid: String, ptid: Int, cid: Int, time: String, func: Int, pwd: String,
                                  code: String, cmd: Any)

case class RemoteCtrlResDataFrame(gid: String, ptid: Int, cid: Int, time: String, func: Int, code: String,
                                  err: Int, info: String)

case class ControlStartStopDataFrame(gid: String, ptid: Int, cid: Int, time: String, func: Int, realSw: Int)


object FuncDefine {
  // 定义功能码
  val HEARTBEATREQUEST: Int = 1 // 心跳请求帧
  val REALTIMEDATA: Int = 2 // 实时数据帧
  val REALTIMESTATU: Int = 3 // 实时状态帧
  val REMOTECONTROLRESPONSE: Int = 4 // 网关接收远程控制命令处理后回应
  val AGPSINFO: Int = 5 //基站定位信息
  val HEARTBEATRESPONSE: Int = 81 // 心跳回复帧
  val TIMESYNC: Int = 82 // 时间同步帧
  val REMOTECONTROLREQUEST: Int = 83 // 远程控制请求帧
  val DATASTARTSTOP: Int = 84 // 控制点表中触发级数据采集
  val POINTSYNC: Int = 85 // 让网关点表保持与服务器一致


  // 第一阶段的func
  val STATEONEFUNC = 10001 // 第一阶段识别码
}