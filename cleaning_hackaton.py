import json

hackatons_to_delete = ["hackathon-eligibility", "hackathon-description-header", "hackathon-requirements", 
                            "hackathon-prizes", "hackathon-judging-criteria", "hackathon-judges", "hackathon-rules"]

participants_to_delete = ["participant-bio", "participant-linkedin", "participant-twitter"]

projects_to_delete = ["project-video"]

def clean_data(source_file, dest_file, parameters_to_delete):
    with open(source_file, 'r') as origin:
        data_dict = json.load(origin)
    for parameter in parameters_to_delete:
        for line in data_dict:
            line.pop(parameter)
    with open(dest_file, 'w') as destination:
        destination.write(json.dumps(data_dict))
    
def main():
    clean_data('./raw_data/all_hackatons.json', './clean_data/cleaned_hackaton.json', hackatons_to_delete)
    clean_data('./raw_data/all_participants.json', './clean_data/cleaned_participants.json', participants_to_delete)
    clean_data('./raw_data/all_projects.json', './clean_data/cleaned_projects.json', projects_to_delete)

if __name__ == "__main__":
    main()


#'./raw_data/all_hackatons.json'
#'./clean_data/cleaned_hackaton.json'
#for line in hackatons_dict:

            


