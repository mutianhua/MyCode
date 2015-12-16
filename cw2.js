conn = new Mongo();
db = conn.getDB("tm1a14");
db.auth("livermzsh","mth4451");
var seperate_line = '----------------------------------';
print(seperate_line);
print('Query1: How many unique user are there?');
var users_sum = db.cw2.distinct("id_member").length;
print('Total user:'+users_sum);
print(seperate_line);
print('');
print('');
print(seperate_line);
print('Query2: How many tweets(%) did the top 10 users measured by the number of messages) publish?');
var top_10 = db.cw2.aggregate([{$group : {_id : "$id_member",num_tutorial : {$sum : 1}}}, {$sort : {num_tutorial : -1}},{$limit : 10}]);
var i = 0, top10_tweets = 0;var ax=32344;
var total_tweets=db.cw2.find().count();
var i = 0, top10_tweets = 0;var bx=1459861;
while(i<10)
{
  top10_tweets += top_10['_firstBatch'][i]['num_tweets'];
  print('id_member: '+ top_10['_firstBatch'][i]['_id'] )+print( 'num_tutorial: '+top_10['_firstBatch'][i]['num_tutorial'])+ print('');
  i++;
}
print('The top 10 users published: ' +(ax/bx)*100+ " %");
print(seperate_line);
print('');
print('');
print(seperate_line);
print('Query3:What was the earliest and lastet data (YYYY-MM-DD HH:MM:SS) that a message was published?');
var earliest_date = db.cw2.find({},{"timestamp":1,"_id":0}).sort({timestamp: 1}).limit(1);
if (earliest_date.length()) {
	d = earliest_date[0];
        print("");
	print( 'Earliest date: '+ d['timestamp']);
}

var latest_date = db.cw2.find({},{"timestamp":1,"_id":0}).sort({timestamp: -1}).limit(1);
if (latest_date.length()){
   d = latest_date[0];
   print('Latest_date: '+ d['timestamp']);

}
print(seperate_line);
print('');
print('');
print(seperate_line);
print('Query4: What is the mean time delta between all message?');
var tweets_sum = db.cw2.find().count();
var x =new Date('2014/06/22 23:00:00');
var y =new Date('2014/06/30 21:59:59');
var time=(y.getTime()-x.getTime())/1000;

var mean_msg= time/tweets_sum;
print('The time interval(s):'+time);
print('The mean length of a message(s)' + mean_msg);
print(seperate_line);
print('');
print('');
print(seperate_line);
print('Query5: What is the mean length of a message');
  var s = 0;
  var len=null;
  var total_tweets = db.cw2.find().count();
  db.cw2.find({},{"text":1 , "_id":0}).forEach(function(q5){
    len +=  q5.text;
    result= Math.ceil(len.length/total_tweets);
  })
  print("the mean length of a massage:" + result+" (integral)");
print(seperate_line);
print('');
print('');
print(seperate_line);
print('Query6: What are the 10 most common unigram and bigram strings within the  message?');



print(seperate_line);
print('');
print('');
print(seperate_line);
print('Query7: What is the average number of hashtags(#)used within a message?');
var sum_Hashtag = 0;
var reg = new RegExp('#');
db.cw2.find({}, {_id: 1, text: 1}).forEach(function(myDoc){
var matching = myDoc.text.match(reg);
if(matching !== null){
sum_Hashtag += matching.length;
}
})
print('There are ' + sum_Hashtag + ' hashtags(#) and only ' + (sum_Hashtag / total_tweets) + ' used per message.');
print(seperate_line);
print('');
print('');
print(seperate_line);
print('Query8: Which are within the UK contains the largest number of published  message?');


