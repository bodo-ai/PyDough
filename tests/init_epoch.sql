-- Custom SQL schema to initialize a custom EPOCH database with tables for
-- historical events and their eras, seasons, and times of day.

CREATE TABLE SEASONS (
  s_name varchar(7) NOT NULL,
  s_month1 INTEGER,
  s_month2 INTEGER,
  s_month3 INTEGER
);

CREATE TABLE TIMES (
  t_name varchar(10) NOT NULL,
  t_start_hour INTEGER,
  t_end_hour INTEGER
);

CREATE TABLE ERAS (
  er_name varchar(11) NOT NULL,
  er_start_year INTEGER,
  er_end_year INTEGER
);

CREATE TABLE EVENTS(
  ev_key INTEGER,
  ev_name varchar (100) NOT NULL,
  ev_dt datetime TIMESTAMP,
  ev_typ varchar(10)
);

CREATE TABLE USERS(
  user_id INTEGER,
  user_name varchar (20) NOT NULL,
  user_region varchar (20) NOT NULL
);

CREATE TABLE SEARCHES(
  search_id INTEGER,
  search_user_id INTEGER,
  search_engine varchar (20) NOT NULL,
  search_string varchar (100) NOT NULL,
  search_ts datetime TIMESTAMP,
  search_num_results INTEGER
);

INSERT INTO SEASONS (s_name, s_month1, s_month2, s_month3) VALUES
('Summer', 6, 7, 8),
('Fall', 9, 10, 11),
('Winter', 12, 1, 2),
('Spring', 3, 4, 5);

INSERT INTO TIMES (t_name, t_start_hour, t_end_hour) VALUES
('Pre-Dawn', 0, 6),
('Morning', 6, 12),
('Afternoon', 12, 18),
('Evening', 18, 21),
('Night', 21, 24);

INSERT INTO ERAS (er_name, er_start_year, er_end_year) VALUES
('WWI', 1914, 1920),
('Interwar', 1920, 1939),
('WWII', 1939, 1946),
('Cold War', 1946, 1991),
('Modern Era', 1991, 2025);

INSERT INTO EVENTS(ev_key, ev_name, ev_dt, ev_typ) VALUES
(1, 'Assassination of Archduke Ferdinand', '1914-06-14 11:02:10', 'death'),
(2, 'Sinking of the Lusitania', '1915-05-07 14:10:00', 'death'),
(3, 'First Zeppelin Raid on London', '1915-01-19 20:00:00', 'war'),
(4, 'First Use of Poison Gas in Warfare', '1915-04-22 17:00:00', 'war'),
(5, 'First Battle of Ypres', '1915-10-19 10:00:00', 'war'),
(6, 'First Battle of the Somme', '1916-07-01 07:30:00', 'war'),
(7, 'Battle of Verdun', '1916-02-21 09:00:00', 'war'),
(8, 'Battle of the Somme', '1916-07-01 07:30:00', 'war'),
(9, 'Battle of the Marne', '1914-09-05 05:00:00', 'war'),
(10, 'Battle of Gallipoli', '1915-04-25 06:30:00', 'war'),
(11, 'Battle of Jutland', '1916-05-31 23:00:00', 'war'),
(12, 'Battle of Verdun', '1916-02-21 09:00:00', 'war'),
(13, 'Battle of the Somme', '1916-07-01 07:30:00', 'war'),
(14, 'Battle of the Marne', '1914-09-05 05:00:00', 'war'),
(15, 'Battle of Gallipoli', '1915-04-25 06:30:00', 'war'),
(16, 'Battle of Jutland', '1916-05-31 23:00:00', 'war'),
(17, 'Battle of Verdun', '1916-02-21 09:00:00', 'war'),
(18, 'Battle of the Somme', '1916-07-01 07:30:00', 'war'),
(19, 'Battle of the Marne', '1914-09-05 05:00:00', 'war'),
(20, 'Battle of Gallipoli', '1915-04-25 06:30:00', 'war'),
(23, 'Battle of Jutland', '1916-05-31 23:00:00', 'war'),
(24, 'Battle of Verdun', '1916-02-21 09:00:00', 'war'),
(25, 'First Transcontinental Phone Call', '1915-01-25 13:45:31', 'science'),
(26, 'Einstein Presents Theory of General Relativity', '1915-11-25 15:00:00', 'science'),
(27, 'United States Enters WWI', '1917-04-04 03:12:00', 'war'),
(28, 'Start of Russian Revolution', '1917-03-18 18:35:00', 'politics'),
(29, 'Treaty of Versailles', '1919-06-28 12:35:00', 'war'),
(30, 'Charlie Chaplin Releases "The Tramp"', '1915-04-11 14:15:00', 'culture'),
(31, 'First Woman Elected to British Parliament (Constance Markievicz)', '1918-12-28 16:00:00', 'politics'),
(32, 'Spanish Flu Pandemic Begins', '1918-01-01 08:00:00', 'health'),
(33, 'First Pulitzer Prizes Awarded', '1917-06-04 00:30:00', 'culture'),
(34, 'Formation of the League of Nations Proposed by Woodrow Wilson', '1918-01-08 10:00:00', 'politics'),

(35, '19th Amendment Ratified', '1920-08-20 11:42:00', 'politics'),
(36, 'Tulsa Massacre', '1921-05-31 10:00:00', 'death'),
(37, 'Black Thursday Wall Street Crash', '1929-10-24 09:30:00', 'economy'),
(38, 'Founding of the League of Nations', '1920-01-10 09:00:00', 'politics'),
(39, 'Discovery of Penicillin by Alexander Fleming', '1928-09-28 13:00:00', 'science'),
(40, 'First Nonstop Transatlantic Flight by Charles Lindbergh', '1927-05-21 22:22:00', 'science'),
(41, 'Stock Market Crash Begins Great Depression', '1929-10-29 20:00:00', 'economy'),
(42, 'Hitler Becomes Leader of the Nazi Party', '1921-07-29 20:00:00', 'politics'),
(43, 'Mahatma Gandhi Begins Salt March', '1930-03-12 06:00:00', 'politics'),
(44, 'First Talking Motion Picture Released', '1927-10-06 02:00:00', 'culture'),
(45, 'Kellogg-Briand Pact Signed', '1928-08-27 07:00:00', 'politics'),
(46, 'First Winter Olympics Held', '1924-01-25 10:00:00', 'sports'),
(47, 'Discovery of Pluto by Clyde Tombaugh', '1930-02-18 13:00:00', 'science'),
(48, 'Japan Invades Manchuria', '1931-09-18 07:00:00', 'war'),
(49, 'Stalin Begins First Five-Year Plan', '1928-10-01 11:00:00', 'politics'),

(50, 'Invasion of Poland', '1939-09-01 04:45:00', 'war'),
(51, 'Attack on Pearl Harbor', '1941-12-07 07:48:00', 'war'),
(52, 'D-Day', '1944-06-06 06:30:00', 'war'),
(53, 'VE Day', '1945-05-08 12:00:00', 'war'),
(54, 'Atomic Bomb Dropped on Hiroshima', '1945-08-06 08:15:00', 'war'),
(55, 'VJ Day', '1945-08-15 12:00:00', 'war'),
(56, 'Nuremberg Trials', '1945-11-20 09:00:00', 'politics'),
(57, 'Battle of Britain', '1940-07-10 09:00:00', 'war'),
(58, 'Operation Barbarossa', '1941-06-22 03:15:00', 'war'),
(59, 'Battle of Stalingrad', '1942-08-23 18:00:00', 'war'),
(60, 'Yalta Conference', '1945-02-04 00:30:00', 'politics'),
(61, 'Potsdam Conference', '1945-07-17 07:00:00', 'politics'),
(62, 'The Blitz Begins', '1940-09-07 20:00:00', 'war'),
(63, 'Formation of the United Nations', '1945-10-24 10:00:00', 'politics'),
(64, 'Anne Frank Goes into Hiding', '1942-07-06 14:00:00', 'culture'),
(65, 'Discovery of Auschwitz Concentration Camp', '1945-01-27 09:00:00', 'war'),
(66, 'Manhattan Project Begins', '1942-06-17 07:00:00', 'science'),
(67, 'First Computer (Colossus) Used for Codebreaking', '1944-02-05 06:15:00', 'science'),
(68, 'Casablanca Conference', '1943-01-14 00:30:00', 'politics'),
(69, 'Battle of Midway', '1942-06-04 00:30:00', 'war'),
(70, 'Liberation of Paris', '1944-08-25 15:00:00', 'war'),
(71, 'Rosie the Riveter Campaign Begins', '1942-01-01 11:00:00', 'culture'),
(72, 'Japanese-American Internment Begins', '1942-02-19 20:00:00', 'politics'),
(73, 'Sinking of the Bismarck', '1941-05-27 07:00:00', 'war'),
(74, 'Battle of El Alamein', '1942-10-23 06:00:00', 'war'),
(75, 'Hitler Commits Suicide', '1945-04-30 23:00:00', 'death'),
(76, 'First Use of V-2 Rockets', '1944-09-08 13:00:00', 'science'),
(77, 'Womens Army Corps (WAC) Established', '1942-05-14 00:30:00', 'politics'),
(78, 'Allied Invasion of Sicily', '1943-07-09 20:00:00', 'war'),
(79, 'Battle of the Bulge', '1944-12-16 05:00:00', 'war'),
(80, 'Surrender of Italy', '1943-09-08 13:00:00', 'politics'),
(81, 'First Meeting of the United Nations General Assembly', '1946-01-10 23:00:00', 'politics'),

(82, 'Start of the Cold War', '1947-03-12 11:00:00', 'politics'),
(83, 'Berlin Blockade and Airlift', '1948-06-24 08:00:00', 'politics'),
(84, 'Formation of NATO', '1949-04-04 10:00:00', 'politics'),
(85, 'Soviet Union Tests First Atomic Bomb', '1949-08-29 07:00:00', 'science'),
(86, 'Korean War Begins', '1950-06-25 05:00:00', 'war'),
(87, 'Rosenbergs Executed for Espionage', '1953-06-19 20:00:00', 'politics'),
(88, 'End of Korean War', '1953-07-27 07:00:00', 'war'),
(89, 'Launch of Sputnik', '1957-10-04 00:30:00', 'science'),
(90, 'Cuban Revolution', '1959-01-01 09:00:00', 'politics'),
(91, 'U-2 Spy Plane Incident', '1960-05-01 14:00:00', 'politics'),
(92, 'Bay of Pigs Invasion', '1961-04-17 07:00:00', 'war'),
(93, 'Berlin Wall Constructed', '1961-08-13 06:00:00', 'politics'),
(94, 'Cuban Missile Crisis', '1962-10-16 10:00:00', 'politics'),
(95, 'Assassination of JFK', '1963-11-22 12:30:00', 'death'),
(96, 'Gulf of Tonkin Incident', '1964-08-02 16:00:00', 'war'),
(97, 'Assassination of Malcolm X', '1965-02-21 03:15:00', 'death'),
(98, 'Assassination of MLK Jr.', '1968-04-04 06:05:00', 'death'),
(99, 'Apollo 11 Moon Landing', '1969-07-20 20:17:00', 'science'),
(100, 'Vietnam War Ends', '1975-04-30 16:00:00', 'war'),
(101, 'Soviet Invasion of Afghanistan', '1979-12-24 08:00:00', 'war'),
(102, 'Fall of the Berlin Wall', '1989-11-09 18:00:00', 'politics'),
(103, 'Chernobyl Nuclear Disaster', '1986-04-26 01:23:00', 'science'),
(104, 'Dissolution of the Soviet Union', '1991-12-26 02:00:00', 'politics'),
(105, 'Woodstock Music Festival', '1969-08-15 06:15:00', 'culture'),
(106, 'First Heart Transplant Performed', '1967-12-03 18:00:00', 'science'),
(107, 'Beatles Release "Sgt. Peppers Lonely Hearts Club Band"', '1967-06-01 11:00:00', 'culture'),
(108, 'Civil Rights Act Signed', '1964-07-02 16:00:00', 'politics'),
(109, 'Stonewall Riots', '1969-06-28 13:00:00', 'politics'),
(110, 'First Email Sent', '1971-10-29 20:00:00', 'science'),
(111, 'Microsoft Founded', '1975-04-04 00:30:00', 'economy'),
(112, 'Apple Founded', '1976-04-01 11:00:00', 'economy'),
(113, 'Iranian Revolution', '1979-02-11 11:00:00', 'politics'),
(114, 'John Lennon Assassinated', '1980-12-08 13:00:00', 'death'),
(115, 'MTV Launches', '1981-08-01 11:00:00', 'culture'),
(116, 'Challenger Disaster', '1986-01-28 11:39:00', 'science'),
(117, 'World Wide Web Invented', '1989-03-12 16:00:00', 'science'),
(118, 'Nelson Mandela Released from Prison', '1990-02-11 12:00:00', 'politics'),
(119, 'Rosa Parks Refuses to Give Up Her Seat', '1955-12-01 11:00:00', 'politics'),
(120, 'Disneyland Opens', '1955-07-17 07:00:00', 'culture'),
(121, 'Alaska Becomes a U.S. State', '1959-01-03 18:00:00', 'politics'),
(122, 'Hawaii Becomes a U.S. State', '1959-08-21 11:00:00', 'politics'),
(123, 'The Beatles Perform on The Ed Sullivan Show', '1964-02-09 20:00:00', 'culture'),
(124, 'First Human in Space (Yuri Gagarin)', '1961-04-12 09:07:00', 'science'),
(125, 'First Woman in Space (Valentina Tereshkova)', '1963-06-16 09:29:00', 'science'),
(126, 'The Voting Rights Act Signed', '1965-08-06 02:00:00', 'politics'),
(127, 'Earth Day Celebrated for the First Time', '1970-04-22 16:00:00', 'culture'),
(128, 'The First Test-Tube Baby Born', '1978-07-25 06:15:00', 'science'),

(129, 'End of Apartheid in South Africa', '1994-04-27 10:00:00', 'politics'),
(130, 'Nelson Mandela Becomes President of South Africa', '1994-05-10 23:00:00', 'politics'),
(131, '9/11 Terrorist Attacks', '2001-09-11 08:46:00', 'terrorism'),
(132, 'Launch of Google', '1998-09-04 14:00:00', 'technology'),
(133, 'Y2K Bug Scare', '2000-01-01 00:01:00', 'technology'),
(134, 'Euro Currency Introduced', '1999-01-01 11:00:00', 'economy'),
(136, 'Hurricane Katrina', '2005-08-29 06:10:00', 'natural_disaster'),
(137, 'Facebook Founded', '2004-02-04 15:00:00', 'technology'),
(138, 'iPhone Released', '2007-06-29 10:00:00', 'technology'),
(139, 'Barack Obama Elected as U.S. President', '2008-11-04 21:00:00', 'politics'),
(140, 'Global Financial Crisis', '2008-09-15 09:00:00', 'economy'),
(141, 'Indian Ocean Tsunami', '2004-12-26 07:58:00', 'natural_disaster'),
(142, 'Mars Rover Spirit Lands on Mars', '2004-01-04 09:00:00', 'science'),
(143, 'Human Genome Project Completed', '2003-04-14 00:30:00', 'science'),
(144, 'Death of Princess Diana', '1997-08-31 11:00:00', 'death'),
(145, 'Columbine High School Shooting', '1999-04-20 23:00:00', 'violence'),
(146, 'War in Afghanistan Begins', '2001-10-07 07:00:00', 'war'),
(147, 'War in Iraq Begins', '2003-03-20 23:00:00', 'war'),
(148, 'Haiti Earthquake', '2010-01-12 16:53:00', 'natural_disaster'),
(149, 'Arab Spring Begins', '2010-12-17 11:30:00', 'politics'),
(150, 'COVID-19 Pandemic Declared', '2020-03-11 11:00:00', 'health'),
(151, 'Brexit Referendum', '2016-06-23 22:00:00', 'politics'),
(152, 'Donald Trump Elected as U.S. President', '2016-11-08 23:00:00', 'politics'),
(153, 'Black Lives Matter Protests Begin', '2013-07-13 14:00:00', 'politics'),
(154, 'First Private Spaceflight by SpaceX', '2012-05-22 13:00:00', 'science'),
(155, 'Bitcoin Created', '2009-01-03 18:15:00', 'economy'),
(156, 'Fukushima Nuclear Disaster', '2011-03-11 14:46:00', 'natural_disaster'),
(157, 'Paris Climate Agreement Signed', '2015-12-12 17:00:00', 'politics'),
(158, 'Me Too Movement Gains Momentum', '2017-10-15 10:00:00', 'culture'),
(159, 'Notre Dame Cathedral Fire', '2019-04-15 18:20:00', 'disaster'),
(160, 'James Webb Space Telescope Launched', '2021-12-25 12:20:00', 'science'),
(161, 'Mars Rover Opportunity Lands on Mars', '2004-01-25 10:30:00', 'science'),
(162, 'YouTube Founded', '2005-02-14 16:00:00', 'technology'),
(163, 'Twitter Launched', '2006-03-21 12:00:00', 'technology'),
(164, 'Pluto Reclassified as a Dwarf Planet', '2006-08-24 11:00:00', 'science'),
(165, 'Michael Jackson Dies', '2009-06-25 14:26:00', 'death'),
(166, 'Osama bin Laden Killed', '2011-05-02 01:00:00', 'war'),
(167, 'Prince William Marries Kate Middleton', '2011-04-29 11:00:00', 'culture'),
(168, 'Edward Snowden NSA Leaks', '2013-06-05 09:00:00', 'politics'),
(169, 'Pope Francis Elected', '2013-03-13 19:00:00', 'religion'),
(170, 'Malala Yousafzai Wins Nobel Peace Prize', '2014-10-10 14:00:00', 'politics'),
(171, 'Leonardo DiCaprio Wins First Oscar', '2016-02-28 22:00:00', 'culture'),
(172, 'Amazon Becomes a Trillion-Dollar Company', '2018-09-04 10:00:00', 'economy'),
(173, 'George Floyd Murder and Protests', '2020-05-25 20:00:00', 'politics'),
(174, 'Joe Biden Elected as U.S. President', '2020-11-07 23:00:00', 'politics'),
(175, 'Russia Invades Ukraine', '2022-02-24 04:00:00', 'war'),
(176, 'Queen Elizabeth II Dies', '2022-09-08 15:10:00', 'death'),
(177, 'ChatGPT Released by OpenAI', '2022-11-30 09:00:00', 'technology'),
(178, 'First Image of a Black Hole Captured', '2019-04-10 13:00:00', 'science'),
(179, 'COVID-19 Vaccines Begin Distribution', '2020-12-14 08:00:00', 'health'),
(180, 'Elon Musk Acquires Twitter', '2022-10-27 14:00:00', 'economy')
;


INSERT INTO USERS(user_id, user_name, user_region) VALUES 
  (1, "Trixie Mattel", "US-Midwest"),
  (2, "Jinkx Monsoon", "US-West"),
  (3, "Alyssa Edwards", "US-South"),
  (4, "Bianca Del Rio", "US-West"),
  (5, "Sasha Velour", "US-East"),
  (6, "Raja Gemini", "US-West"),
  (7, "Roxxxie Andrews", "US-South"),
  (8, "Plasma", "US-East"),
  (9, "Priyanka", "Canada"),
  (10, "Jimbo", "Canada"),
  (11, "The Vivienne", "UK"),
  (12, "Lemon", "Canada"),
  (13, "Gigi Goode", "US-West"),
  (14, "Elektra Fence", "US-South"),
  (15, "Kylie Sonique Love", "US-South"),
  (16, "Jorgeous", "US-West"),
  (17, "Kandy Muse", "US-East"),
  (18, "Angeria Paris VanMichaels", "US-South"),
  (19, "Willow Pill", "US-West"),
  (20, "Lady Camden", "UK")
;

INSERT INTO SEARCHES(search_id, search_user_id, search_engine, search_string, search_ts, search_num_results) VALUES 
  (1, 1, "Google", "When did covid-19 vaccines begin distribution?", "2024-12-31 23:00:13", 142),
  (2, 5, "Bing", "When was the start of the COLD WAR!?!?", "2018-01-01 10:35:00", 186),
  (3, 2, "Google", "When was the first test-tube baby born?", "2019-01-10 23:45:00", 192),
  (4, 20, "Duck Duck Go", "THE FIRST TEST-TUBE BABY BORN", "2020-06-01 00:00:00", 3),
  (5, 16, "Google", "When was the first test-tube baby born in Canada", "2021-01-05 05:51:56", 200),
  (6, 1, "Yahoo!", "On what day of the year was the first test-tube baby born?", "2018-03-25 06:00:00", 13),
  (7, 1, 'Google', 'Searching for The First Voyage of Columbus', '2018-06-12 14:23:45', 4523),
  (8, 2, 'Bing', 'Details about First Crusade and Godfrey of Bouillon', '2020-11-04 09:14:12', 7381),
  (9, 3, 'DuckDuckGo', 'Fall of Constantinople and its consequences', '2017-02-27 20:10:33', 104),
  (10, 1, 'Google', 'Looking into the y2k bug scare causes', '2022-09-19 16:47:01', 9900),
  (11, 2, 'Yahoo', 'What year did The Beatles perform on The Ed Sullivan Show?', '2015-01-08 08:00:00', 8457),
  (12, 7, 'Bing', 'impact of bitcoin created on the Edward Snowden NSA leak sand the Indian Ocean tsunami', '2016-08-29 22:45:30', 8744),
  (13, 9, 'DuckDuckGo', 'columbus first voyage details and legacy', '2019-12-04 13:33:09', 2203),
  (14, 10, 'Yahoo', 'summary of why pluto reclassified as a planet', '2023-01-20 06:59:55', 5460),
  (15, 6, 'Google', 'why did the first crusade happen?', '2015-07-11 17:17:48', 9281),
  (16, 11, 'Bing', 'explore the events surrounding the casablanca conference', '2021-04-08 08:41:03', 7895),
  (17, 14, 'DuckDuckGo', 'lessons from the great fire of london', '2018-10-23 11:19:27', 643),
  (18, 5, 'Yahoo', 'when did the Black Lives Matter protests begin and how long did they last', '2017-09-01 20:00:00', 8742),
  (19, 18, 'Google', 'War in Iraq begins vs ARAB SPRING begins', '2022-12-12 14:02:21', 9900),
  (20, 13, 'Bing', 'analyzing the Chernobyl nuclear disaster', '2016-04-30 09:18:39', 4628),
  (21, 3, 'Ecosia', 'who caused the cuban missile crisis and who averted it?', '2020-11-02 15:12:34', 1289),
  (22, 8, 'Google', 'black death mortality rate and spread in europe', '2019-05-09 12:45:00', 2745),
  (23, 1, 'Brave', 'timeline of Disneyland opens and its impact on Florida', '2023-07-01 07:22:49', 883),
  (24, 17, 'DuckDuckGo', 'causes of the haitian revolution', '2022-09-19 19:05:14', 2297),
  (25, 6, 'Bing', 'historical analysis of the y2k bug scare in America', '2016-01-11 16:17:59', 6213),
  (26, 2, 'Qwant', 'short term effects of civil rights act signed and the stonewall riots', '2021-03-30 20:08:25', 1921),
  (27, 20, 'Yahoo', 'storming of the bastille vs boston tea party', '2024-02-18 14:44:39', 4021),
  (28, 13, 'Google', 'legacy of the brexit referendum', '2017-06-15 23:05:12', 1754),
  (29, 10, 'Ecosia', 'who was at the death of princess diana?', '2018-12-27 05:17:46', 1010),
  (30, 7, 'Google', 'importance of the edict of milan to early christianity', '2015-10-10 10:10:10', 234),
  (31, 5, 'Brave', 'napoleon at the battle of waterloo and tactical errors', '2023-04-22 17:38:29', 6915),
  (32, 12, 'DuckDuckGo', 'why did the taiping rebellion fail?', '2020-08-14 09:00:45', 1783),
  (33, 19, 'Google', 'timeline of the meiji restoration', '2019-02-03 13:11:12', 2989),
  (34, 14, 'Yahoo', 'causes and consequences of the boxer rebellion', '2021-05-06 18:22:56', 2180),
  (35, 1, 'Qwant', 'how did the formation of the united nations affect european trade?', '2017-01-01 08:33:00', 3427),
  (36, 9, 'Bing', 'major between when the War in Afghanistan begins and the War in Iraq begins', '2018-07-19 20:40:14', 901),
  (37, 4, 'Google', 'significance of the magna carta in english history', '2022-06-13 11:26:32', 5609),
  (38, 16, 'DuckDuckGo', 'differences between the french and russian revolutions', '2023-03-25 21:00:00', 792),
  (39, 18, 'Ecosia', 'background to the signing of the camp david accords', '2020-10-02 04:45:01', 258),
  (40, 15, 'Brave', 'alexander the great and the battle of gaugamela', '2024-01-08 16:50:22', 3820),
  (41, 6, 'Google', 'why was the treaty of versailles controversial', '2019-11-17 12:45:33', 2450),
  (42, 2, 'DuckDuckGo', 'origin story behind the trojan war', '2018-04-23 06:12:14', 992),
  (43, 10, 'Ecosia', 'battle tactics at cannae by hannibal', '2021-02-11 19:55:21', 1540),
  (44, 13, 'Bing', 'how the industrial revolution changed child labor laws', '2020-01-30 15:10:45', 3647),
  (45, 7, 'Qwant', 'who was present at the signing of the magna carta', '2017-07-05 08:32:10', 173),
  (46, 12, 'Google', 'why did the y2k bug scare not cause an economic collapse', '2022-05-28 20:13:39', 2044),
  (47, 4, 'Mojeek', 'the sacking of rome by the visigoths', '2023-01-10 17:07:07', 320),
  (48, 5, 'Yahoo', 'timeline of events leading to pearl harbor', '2016-06-16 10:00:00', 5891),
  (49, 14, 'Brave', 'casualties during the siege of leningrad', '2021-09-03 23:19:05', 2705),
  (50, 19, 'Google', 'reforms implemented in the meiji era', '2018-03-09 14:01:50', 1341),
  (51, 1, 'Yandex', 'reasons napoleon lost waterloo', '2017-10-21 12:55:43', 4012),
  (52, 15, 'DuckDuckGo', 'impact of Hurricane Katrina on ecosystems in Louisiana', '2020-08-08 08:08:08', 299),
  (53, 3, 'Ecosia', 'When was the James Webb Space Telescope Launched?', '2019-02-28 17:03:12', 583),
  (54, 1, 'Swisscows', 'economic aftermath of the Challenger disaster', '2023-06-14 11:22:56', 712),
  (55, 8, 'Google', 'partition of india effects on migration', '2024-03-10 16:17:00', 5017),
  (56, 20, 'Yahoo', 'Was the first heart transplant performed before or after the woodstock music festival', '2021-12-12 09:09:09', 1478),
  (57, 16, 'Bing', 'Leonardo DiCaprio wins first oscar', '2022-02-02 18:30:30', 2521),
  (58, 17, 'Google', 'what caused the dust bowl in the 1930s', '2020-10-15 06:43:22', 980),
  (59, 9, 'Qwant', 'fall of constantinople byzantine defenses', '2019-08-20 13:50:40', 1222),
  (60, 18, 'DuckDuckGo', 'vikings and the raid on lindisfarne', '2016-11-01 04:04:04', 2334),
  (61, 2, 'Brave', 'legacy of the french revolution on democracy', '2017-04-14 21:17:35', 1899),
  (62, 10, 'Ecosia', 'unification of germany under bismarck', '2023-09-26 07:45:11', 3266),
  (63, 6, 'Google', 'factors in the y2k bug scare', '2022-01-01 12:00:00', 541),
  (64, 13, 'Mojeek', 'invention of the printing press by gutenberg', '2021-04-03 14:04:14', 1123),
  (65, 19, 'Bing', 'enlightenment ideas that inspired revolutions', '2018-12-05 16:19:59', 2738),
  (66, 14, 'Google', 'what happened during the great fire of london', '2020-07-07 05:20:35', 778),
  (67, 7, 'DuckDuckGo', 'salem witch trials hysteria explained', '2015-09-18 19:13:10', 332),
  (68, 12, 'Yahoo', 'when did julius caesar cross the rubicon', '2024-04-01 01:01:01', 1190),
  (69, 3, 'Google', 'economic impact of the berlin wall', '2016-02-26 11:11:11', 2311),
  (70, 4, 'Brave', 'cultural importance of the renaissance in italy', '2017-06-30 10:25:45', 3478),
  (71, 11, 'Qwant', 'Did the Mars Rover Spirit lands on Mars or Jupiter?', '2019-10-10 22:22:22', 209),
  (72, 5, 'Yandex', 'armenian genocide recognition worldwide', '2021-03-15 12:40:20', 189),
  (73, 15, 'Google', 'europe after the thirty years war', '2020-01-05 07:33:33', 894),
  (74, 8, 'Swisscows', 'key figures in ensuring Osama Bin Laden killed', '2023-08-08 18:18:18', 1987),
  (75, 1, 'DuckDuckGo', 'how the columbian exchange changed the world', '2022-11-11 11:11:11', 304),
  (76, 17, 'Ecosia', 'long-term effects of the battle of hastings', '2015-05-05 05:05:05', 602),
  (77, 20, 'Google', 'timeline of the iranian revolution', '2019-01-09 16:00:00', 4069),
  (78, 18, 'Yahoo', 'was was the economic impact of when ChatGPT released by OpenAI', '2021-08-21 09:45:00', 1480),
  (79, 16, 'Mojeek', 'causes of the tulsa massacre', '2020-09-10 14:15:00', 273),
  (80, 9, 'Google', 'when is VE day', '2023-02-02 02:02:02', 6445),
  (81, 19, 'Qwant', 'Why did the assassination of archduke ferdinand cause WWI', '2024-02-24 12:12:12', 382),
  (82, 3, 'Bing', 'What is VJ day and why is it celebrated', '2018-06-06 08:00:00', 1777),
  (83, 6, 'Google', 'why did the treaty of versailles lead to the invasion of poland and the attack on pearl harbor', '2022-07-14 11:30:30', 1215),
  (84, 13, 'DuckDuckGo', 'how many died in the Haiti earthquake', '2020-03-03 03:03:03', 487),
  (85, 10, 'Ecosia', 'civil rights movement and the march on washington', '2017-08-28 14:00:00', 3128),
  (86, 12, 'Brave', 'what caused the fall of the berlin wall?', '2019-12-12 23:23:23', 821),
  (87, 7, 'Swisscows', 'agricultural changes from the neolithic revolution', '2023-05-05 09:09:09', 247),
  (88, 4, 'Google', 'how soon after black thursday wall street crash was the onset of the great depression globally', '2016-03-19 04:40:40', 6881),
  (89, 5, 'Yahoo', 'how did the abolition of slavery happen in britain', '2018-10-01 10:10:10', 930),
  (90, 14, 'DuckDuckGo', 'origin and result of the boxer rebellion', '2021-10-10 20:20:20', 1794),
  (91, 11, 'Qwant', 'why did the brexit referendum happen', '2020-06-06 12:12:12', 633),
  (92, 16, 'Ecosia', 'history of the george floyd murder and protests', '2017-03-20 06:45:00', 2039),
  (93, 8, 'Google', 'role of women during how Hawaii becomes a U.S. state', '2019-09-09 15:15:15', 5177),
  (94, 15, 'Mojeek', 'cultural significance of the olympic games in greece', '2022-04-04 04:04:04', 371),
  (95, 2, 'Brave', 'who built the great wall of china and why', '2015-01-01 01:01:01', 2766),
  (96, 9, 'Yandex', 'cold war tensions during the cuban missile crisis', '2016-08-08 17:00:00', 4867),
  (97, 18, 'Google', 'when was the end of apartheid in south africa', '2023-12-12 06:06:06', 2444),
  (98, 1, 'DuckDuckGo', 'causes and aftermath of the russo-japanese war', '2021-01-15 15:45:45', 939),
  (99, 20, 'Google', 'philosophy in ancient athens', '2022-02-20 10:10:10', 148),
  (100, 17, 'Yahoo', 'how the reformation changed europe forever', '2020-05-01 08:08:08', 1888),
  (101, 5, 'Google', 'best medieval recipes using barley', '2021-06-15 08:00:00', 841),
  (102, 8, 'DuckDuckGo', 'astronomy discoveries of the 1600s', '2023-04-02 21:33:12', 163),
  (103, 13, 'Ecosia', 'when was youtube founded', '2018-12-20 11:20:00', 394),
  (104, 7, 'Bing', 'understanding feudal contracts', '2020-11-09 17:17:17', 227),
  (105, 10, 'Swisscows', 'MANHATTAN PROJECT BEGINS', '2022-03-21 13:45:00', 612),
  (106, 2, 'Qwant', 'strange facts about the time when Twitter launched', '2019-07-08 14:22:45', 79),
  (107, 16, 'Brave', 'why did elon musk acquires Twitter?', '2023-08-01 16:00:00', 503),
  (108, 14, 'Yahoo', 'biography of an unknown monk from 1200s', '2017-02-10 10:10:10', 42),
  (109, 3, 'Google', 'styles of greek pottery', '2016-10-23 03:30:00', 395),
  (110, 11, 'Mojeek', 'forgotten queens of the pre-columbian americas', '2020-01-01 05:00:00', 177),
  (111, 1, 'Google', 'Hawaii Becomes a U.S. State and the siege of leningrad compared', '2018-09-14 07:47:11', 1044),
  (112, 6, 'DuckDuckGo', 'trojan war and the fall of troy in literature', '2021-12-25 13:45:30', 198),
  (113, 9, 'Yandex', 'is the way the vietnam war ends in the X-Men movies real?', '2019-05-05 22:22:22', 830),
  (114, 4, 'Ecosia', 'battle of hastings and the magna carta legacy', '2022-06-19 18:00:00', 633),
  (115, 15, 'Brave', 'when was the first email sent and who sent it', '2020-03-03 15:10:30', 742),
  (116, 18, 'Google', 'connection between the fall of rome and the renaissance', '2023-10-10 10:10:10', 1692),
  (117, 20, 'Yahoo', 'berlin wall and the cold war breakdown', '2021-07-30 20:45:00', 905),
  (118, 12, 'DuckDuckGo', 'from the french revolution to the napoleonic wars', '2017-08-08 09:09:09', 1221),
  (119, 17, 'Qwant', 'timeline between Alaska Becomes a U.S. State and Hawaii Becomes a U.S. State', '2019-01-01 00:00:00', 2101),
  (120, 19, 'Google', 'queen elizabeth II dies and protestant reformation linked', '2024-02-02 11:11:11', 514),
  (121, 4, 'Google', 'was the fall of constantinople inevitable?', '2020-03-15 10:05:12', 3912)
;

-- SELECT USERS.user_name, COUNT(DISTINCT SEARCHES.search_id)
-- FROM USERS, SEARCHES, EVENTS, ERAS
-- WHERE USERS.user_id = SEARCHES.search_user_id
-- AND LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
-- AND CAST(STRFTIME('%Y', EVENTS.ev_dt) AS INTEGER) >= ERAS.er_start_year
-- AND CAST(STRFTIME('%Y', EVENTS.ev_dt) AS INTEGER) < ERAS.er_end_year
-- AND ERAS.er_name = 'Cold War'
-- GROUP BY 1
-- ORDER BY 2 DESC
-- ;

-- SELECT t_name AS tod, (100.0 * COUNT(*)) / 121.0 AS n_searches
-- FROM SEARCHES, TIMES
-- WHERE CAST(STRFTIME('%H', search_ts) AS INTEGER) >= t_start_hour
-- AND CAST(STRFTIME('%H', search_ts) AS INTEGER) < t_end_hour
-- GROUP BY 1
-- ;

-- SELECT s_name AS season_name, (100.0 * SUM(CASE WHEN CAST(STRFTIME('%m', ev_dt) AS INTEGER) IN (SEASONS.s_month1, SEASONS.s_month2, SEASONS.s_month3) THEN 1 END)) / COUNT(*) as search_pct
-- FROM SEASONS, SEARCHES, EVENTS
-- WHERE CAST(STRFTIME('%m', search_ts) AS INTEGER) IN (SEASONS.s_month1, SEASONS.s_month2, SEASONS.s_month3)
-- AND LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
-- GROUP BY 1
-- ORDER BY 1
-- ;

-- SELECT s_name AS season_name, (100.0 * SUM(CASE WHEN CAST(STRFTIME('%m', search_ts) AS INTEGER) IN (SEASONS.s_month1, SEASONS.s_month2, SEASONS.s_month3) THEN 1 END)) / COUNT(*) as search_pct
-- FROM SEASONS, SEARCHES, EVENTS
-- WHERE CAST(STRFTIME('%m', ev_dt) AS INTEGER) IN (SEASONS.s_month1, SEASONS.s_month2, SEASONS.s_month3)
-- AND LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
-- GROUP BY 1
-- ORDER BY 1
-- ;

-- SELECT user_region, ev_typ, n_searches
-- FROM (
--   select user_region, EVENTS.ev_typ, COUNT(DISTINCT searches.search_id) AS n_searches,
--   ROW_NUMBER() OVER (PARTITION BY user_region ORDER BY COUNT(DISTINCT searches.search_id) DESC, ev_typ ASC) AS rn
--   FROM USERS, SEARCHES, EVENTS
--   WHERE LOWER(SEARCHES.search_string) LIKE CONCAT('%', LOWER(EVENTS.ev_name), '%')
--   AND USERS.user_id = SEARCHES.search_user_id
--   GROUP BY 1, 2
-- )
-- WHERE rn = 1
-- ORDER BY 1
-- ;


-- SELECT t_name, search_engine, n_searches
-- FROM (
--   select TIMES.t_name, SEARCHES.search_engine, COUNT(*) AS n_searches,
--   ROW_NUMBER() OVER (PARTITION BY t_name ORDER BY COUNT(*) DESC, search_engine ASC) AS rn
--   FROM TIMES, SEARCHES
--   WHERE CAST(STRFTIME('%H', search_ts) AS INTEGER) >= t_start_hour
--   AND CAST(STRFTIME('%H', search_ts) AS INTEGER) < t_end_hour
--   GROUP BY 1, 2
-- )
-- WHERE rn = 1
-- ORDER BY 1
-- ;