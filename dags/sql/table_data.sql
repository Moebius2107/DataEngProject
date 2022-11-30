-- populate table
-- COPY HACKATON_DETAILS FROM 'C:\Users\jinfr\projetData\DataEngProject\data\data_to_prod.csv' DELIMITER ','CSV;

COPY hackaton_details(
	index,
	hackathon_id,
    hackathon_winner,
    hackathon_url_caller,
    project_title,
           project_likes,
           hackathon_name,
           project_subtitle,
           project_github_url,
					  project_number_of_comments,    
        team_size,   
        project_creation_timestamp,     
        project_url,   
        project_id,   
        team,   
        participant_id,   
        _id,   
        hackathon_number_of_prizes,    
        hackathon_number_of_judges,   
        hackathon_number_of_participants,   
        hackathon_is_colocated,   
        hackathon_has_ended,   
        hackathon_url,   
        hackathon_description_header,    
        hackathon_prizes_total,   
        has_projects,    
        Country,   
        participant_likes,   
        participant_projects,   
        participant_url,  
        participant_name,   
        participant_followers,   
        participant_location, 
        participant_bio,
        participant_linkedin,  
        participant_website,   
        participant_github,   
        participant_hackathons,   
        participant_following,   
        participant_number_of_skills
				  ) FROM 'C:\Program Files\PostgreSQL\15\data_to_prod.csv' CSV HEADER;

CREATE TABLE dim_participants AS
  SELECT participant_id,   
        participant_likes,   
        participant_projects,   
        participant_url,  
        participant_name,   
        participant_followers,   
        participant_location, 
        participant_bio,
        participant_linkedin,  
        participant_website,   
        participant_github,   
        participant_hackathons,   
        participant_following,   
        participant_number_of_skills 
    FROM hackaton_details;

CREATE TABLE facts AS
  SELECT hackathon_id,
    project_id, 
    participant_id 
    FROM hackaton_details;

CREATE TABLE dim_hackatons AS
    SELECT hackathon_id,
    hackathon_winner,
    hackathon_url_caller, 
	hackathon_number_of_prizes,    
        hackathon_number_of_judges,   
        hackathon_number_of_participants,   
        hackathon_is_colocated,   
        hackathon_has_ended,   
        hackathon_url,   
        hackathon_description_header,    
        hackathon_prizes_total,
		Country
		FROM hackaton_details;

CREATE TABLE dim_projects AS
  SELECT project_title,
           project_likes,
		   project_subtitle,
           project_github_url,
			project_number_of_comments,    
        team_size,   
        project_creation_timestamp,     
        project_url,   
        project_id,   
        team 
		FROM hackaton_details;