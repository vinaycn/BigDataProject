package models

/**
  * Created by vinay on 4/4/17.
  */
case class User(id: Long,name:String, email: String,  age :Int,password:String)


case class UserFormData(name:String, age :Int, email: String, password:String)



