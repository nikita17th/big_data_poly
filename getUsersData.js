var users = API.users.get({"user_ids":Args.user_ids,"fields":Args.fields});
var userIds = users@.id;
var index = 0;
var allPhotos = [];
while(index < userIds.length) {
    var photos = API.photos.get({"owner_id":userIds[index],"album_id":"profile"});
    allPhotos.push({"user_id":userIds[index],"photos":photos.items@.date});
    index = index + 1;
}
return {"users":users,"photos_dates":allPhotos};