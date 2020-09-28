package com.xiaolin.sparkStream

import scala.io.Source

/**
 * @program: spark-scala001
 * @description:
 * @author: linzy
 * @create: 2020-09-24 15:48
 **/
object GuPiao {

  def main(args: Array[String]): Unit = {

    println("查询新浪股票（每小时更新） http://hq.sinajs.cn/list=sh601006,sh601007")
    val sinaStockStream = Source.fromURL("http://hq.sinajs.cn/list=sh601006,sh601007", "gbk")
    val sinaLines = sinaStockStream.getLines

    for (line <- sinaLines) {
      println(SinaStock(line).toString)
    }
    sinaStockStream.close()

  }

}
object SinaStock {

  def apply(stockInfo:String) :SinaStock = {
    new SinaStock(stockInfo)
  }

}

class SinaStock {

  var code:String="" //“sh601006”，股票代码
  var name :String =""  //”大秦铁路”，股票名字
  var curOpenPrice :Float =0 //”27.55″，今日开盘价
  var lstOpenPrice:Float =0 //”27.25″，昨日收盘价
  var curPrice :Float =0 //”26.91″，当前价格
  var highestPrice  :Float =0 //”27.55″，今日最高价
  var lowestPrice :Float=0 //”26.20″，今日最低价
  var bidBuyPrice:Float=0 //”26.91″，竞买价，即“买一”报价
  var bidSalePrice:Float=0 //”26.92″，竞卖价，即“卖一”报价
  var dealNum :Long=0 //8：”22114263″，成交的股票数，由于股票交易以一百股为基本单位，所以在使用时，通常把该值除以一百
  var dealAmount  :Float=0 //9：”589824680″，成交金额，单位为“元”，为了一目了然，通常以“万元”为成交金额的单位，所以通常把该值除以一万
  var bidBuy1Num :Long=0 //10：”4695″，“买一”申请4695股，即47手
  var bidBuy1Amount :Float=0 //11：”26.91″，“买一”报价
  var bidBuy2Num :Long=0
  var bidBuy2Amount :Float=0
  var bidBuy3Num :Long=0
  var bidBuy3Amount :Float=0
  var bidBuy4Num :Long=0
  var bidBuy4Amount :Float=0
  var bidBuy5Num :Long=0
  var bidBuy5Amount :Float=0
  var bidSale1Num :Long=0 //“卖一”申报3100股，即31手
  var bidSale1Amount :Float=0 //“卖一”报价
  var bidSale2Num :Long=0
  var bidSale2Amount :Float=0
  var bidSale3Num :Long=0
  var bidSale3Amount :Float=0
  var bidSale4Num :Long=0
  var bidSale4Amount :Float=0
  var bidSale5Num :Long=0
  var bidSale5Amount :Float=0
  var date:String ="" //”2008-01-11″，日期
  var time:String="" //”15:05:32″，时间
  def toDebugString =  "code[%s],name[%s],curOpenPrice [%f],lstOpenPrice[%f],curPrice [%f],highestPrice  [%f],lowestPrice [%f],bidBuyPrice[%f],bidSalePrice[%f],dealNum [%d],dealAmount  [%f],bidBuy1Num [%d],bidBuy1Amount [%f],,bidBuy2Num [%d],bidBuy2Amount [%f],bidBuy3Num [%d],bidBuy3Amount [%f],bidBuy4Num [%d],bidBuy4Amount [%f],bidBuy5Num [%d],bidBuy5Amount [%f],bidSale1Num [%d],bidSale1Amount [%f],bidSale2Num [%d],bidSale2Amount [%f],bidSale3Num [%d],bidSale3Amount [%f],bidSale4Num [%d],bidSale4Amount [%f],bidSale5Num [%d],bidSale5Amount [%f],date [%s],time [%s]" .format( this.code,    this.name,    this.curOpenPrice ,    this.lstOpenPrice,    this.curPrice ,    this.highestPrice  ,    this.lowestPrice ,    this.bidBuyPrice,    this.bidSalePrice,    this.dealNum ,    this.dealAmount  ,    this.bidBuy1Num ,    this.bidBuy1Amount ,    this.bidBuy2Num ,    this.bidBuy2Amount ,    this.bidBuy3Num ,    this.bidBuy3Amount ,    this.bidBuy4Num ,    this.bidBuy4Amount ,    this.bidBuy5Num ,    this.bidBuy5Amount ,    this.bidSale1Num ,    this.bidSale1Amount ,    this.bidSale2Num ,    this.bidSale2Amount ,    this.bidSale3Num ,    this.bidSale3Amount ,    this.bidSale4Num ,    this.bidSale4Amount ,    this.bidSale5Num ,    this.bidSale5Amount ,    this.date ,    this.time  )
  override def toString =  Array(this.code,this.name,this.curOpenPrice,this.lstOpenPrice,this.curPrice,this.highestPrice,this.lowestPrice,this.bidBuyPrice,this.bidSalePrice,this.dealNum,this.dealAmount,this.bidBuy1Num,this.bidBuy1Amount,this.bidBuy2Num,this.bidBuy2Amount,this.bidBuy3Num,this.bidBuy3Amount,this.bidBuy4Num,this.bidBuy4Amount,this.bidBuy5Num,this.bidBuy5Amount,this.bidSale1Num,this.bidSale1Amount,this.bidSale2Num,this.bidSale2Amount,this.bidSale3Num,this.bidSale3Amount,this.bidSale4Num,this.bidSale4Amount,this.bidSale5Num,this.bidSale5Amount,this.date,this.time).mkString(",")
  private var stockInfo :String =""
  def getStockInfo = stockInfo

  def this(stockInfo:String) {
    this()
    this.stockInfo=stockInfo

    val stockDetail=stockInfo.split(Array(' ','_','=',',','"'))
    if (stockDetail.length>36){
      this.code=stockDetail(3)
      this.name=stockDetail(5)
      this.curOpenPrice =stockDetail(6).toFloat
      this.lstOpenPrice=stockDetail(7).toFloat
      this.curPrice =stockDetail(8).toFloat
      this.highestPrice  =stockDetail(9).toFloat
      this.lowestPrice =stockDetail(10).toFloat
      this.bidBuyPrice=stockDetail(11).toFloat
      this.bidSalePrice=stockDetail(12).toFloat
      this.dealNum =stockDetail(13).toLong
      this.dealAmount  =stockDetail(14).toFloat
      this.bidBuy1Num =stockDetail(15).toLong
      this.bidBuy1Amount =stockDetail(16).toFloat
      this.bidBuy2Num =stockDetail(17).toLong
      this.bidBuy2Amount =stockDetail(18).toFloat
      this.bidBuy3Num =stockDetail(19).toLong
      this.bidBuy3Amount =stockDetail(20).toFloat
      this.bidBuy4Num =stockDetail(21).toLong
      this.bidBuy4Amount =stockDetail(22).toFloat
      this.bidBuy5Num =stockDetail(23).toLong
      this.bidBuy5Amount =stockDetail(24).toFloat
      this.bidSale1Num =stockDetail(25).toLong
      this.bidSale1Amount =stockDetail(26).toFloat
      this.bidSale2Num =stockDetail(27).toLong
      this.bidSale2Amount =stockDetail(28).toFloat
      this.bidSale3Num =stockDetail(29).toLong
      this.bidSale3Amount =stockDetail(30).toFloat
      this.bidSale4Num =stockDetail(31).toLong
      this.bidSale4Amount =stockDetail(32).toFloat
      this.bidSale5Num =stockDetail(33).toLong
      this.bidSale5Amount =stockDetail(34).toFloat
      this.date =stockDetail(35)
      this.time =stockDetail(36)
    }
  }
}
