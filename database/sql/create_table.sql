-- create a table to store the channelID and channelTitle
CREATE TABLE IF NOT EXISTS "channel" (
    channel_id TEXT NOT NULL PRIMARY KEY,
    playlist_id TEXT NOT NULL,
    title TEXT NOT NULL
); 

 -- create a table to store vide id and channelID
CREATE TABLE IF NOT EXISTS video (
    channel_id TEXT NOT NULL REFERENCEs channel(channel_id),
    video_id TEXT NOT NULL PRIMARY KEY,
    title VARCHAR(100),
    video_description VARCHAR(200),
    published_time TIMESTAMP ,
    view_count BIGINT NOT NULL CHECK(view_count>0),
    like_count BIGINT NOT NULL CHECK(view_count>0)
);  

  -- create a table to store the video which had been watched
CREATE TABLE IF NOT EXISTS history (
    id INT GENERATED ALWAYS AS IDENTITY,
    dt DATE NOT NULL, 
    video_id TEXT NOT NULL REFERENCEs video(video_id) 
);
