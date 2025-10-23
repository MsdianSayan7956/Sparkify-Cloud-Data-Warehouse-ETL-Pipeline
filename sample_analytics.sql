
-- SPARKIFY DATA WAREHOUSE - SAMPLE ANALYTICAL QUERIES
-- ====================================================


-- 1. MOST POPULAR SONGS
-- 1. What are our top 10 most played songs?

SELECT 
    s.title,
    a.name as artist_name,
    COUNT(*) as play_count,
    ROUND(AVG(s.duration), 2) as avg_duration_seconds
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY s.title, a.name, s.duration
ORDER BY play_count DESC
LIMIT 10;



-- 2. PEAK USAGE ANALYSIS BY HOUR

SELECT 
    t.hour,
    COUNT(*) as total_plays,
    COUNT(DISTINCT sp.user_id) as unique_users,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT sp.user_id), 2) as avg_plays_per_user
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY t.hour;


-- 3. USER ENGAGEMENT: PAID VS FREE USERS

SELECT 
    level,
    COUNT(*) as total_plays,
    COUNT(DISTINCT user_id) as total_users,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT user_id), 2) as avg_plays_per_user,
    ROUND(AVG(CASE WHEN song_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100, 1) as match_rate_percent
FROM songplays
GROUP BY level
ORDER BY level;


-- 4. TOP ARTISTS BY PLAY COUNT

SELECT 
    a.name as artist_name,
    COUNT(*) as total_plays,
    COUNT(DISTINCT s.song_id) as unique_songs,
    COUNT(DISTINCT sp.user_id) as unique_listeners,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT s.song_id), 2) as avg_plays_per_song
FROM songplays sp
JOIN artists a ON sp.artist_id = a.artist_id
JOIN songs s ON sp.song_id = s.song_id
GROUP BY a.name
HAVING COUNT(*) >= 5  -- Only artists with 5+ plays
ORDER BY total_plays DESC
LIMIT 15;




-- 5. GEOGRAPHIC DISTRIBUTION

SELECT 
    location,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as total_plays,
    ROUND(COUNT(*)::FLOAT / COUNT(DISTINCT user_id), 2) as avg_plays_per_user
FROM songplays
WHERE location IS NOT NULL
GROUP BY location
HAVING COUNT(DISTINCT user_id) >= 2  -- Locations with 2+ users
ORDER BY total_plays DESC
LIMIT 10;


