package com.ecommerce

import scala.beans.BeanProperty

package object hotItemBean {

  //输入数据样例类
  @BeanProperty
  case class UserBehavior(userId: Long, itemId: Long,  categoryId: Int, behavior: String, timestamp: Long)

  //窗口聚合结果样例类
  @BeanProperty
  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

}
