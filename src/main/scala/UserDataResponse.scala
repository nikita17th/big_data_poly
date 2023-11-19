import spray.json._

case class UserDataResponse(response: UserDataList, execute_errors: Seq[ExecuteError])

case class UserDataList(users: Seq[UserData],
                        photos_dates: Seq[UserPhotos])

case class UserData(id: Int,
                    city: Option[City],
                    country: Option[Country],
                    first_name: Option[String],
                    last_name: Option[String],
                    last_seen: Option[LastSeen],
                    can_access_closed: Option[Boolean],
                    is_closed: Option[Boolean],
                    photos: Option[Seq[Int]],
                    deactivated: Option[String])

case class LastSeen(time: Long, platform: Option[Int])

case class City(id: Option[Int], title: Option[String])

case class Country(id: Option[Int], title: Option[String])

case class UserPhotos(user_id: Int, photos: Seq[Int])

case class ExecuteError(method: String, error_code: Int, error_msg: String)

object UserDataJsonProtocol extends DefaultJsonProtocol {
  implicit val lastSeenFormat: RootJsonFormat[LastSeen] = jsonFormat2(LastSeen)
  implicit val cityFormat: RootJsonFormat[City] = jsonFormat2(City)
  implicit val countryFormat: RootJsonFormat[Country] = jsonFormat2(Country)
  implicit val userDataFormat: RootJsonFormat[UserData] = jsonFormat10(UserData)
  implicit val userPhotosFormat: RootJsonFormat[UserPhotos] = jsonFormat2(UserPhotos)
  implicit val userDataListFormat: RootJsonFormat[UserDataList] = jsonFormat2(UserDataList)
  implicit val executeErrorFormat: RootJsonFormat[ExecuteError] = jsonFormat3(ExecuteError)
  implicit val userDataResponseFormat: RootJsonFormat[UserDataResponse] = jsonFormat2(UserDataResponse)
}
