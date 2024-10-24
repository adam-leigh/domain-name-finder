import pandas as pd
import whois
import os
import time
import logging

def load_dataframe(file_path):
    """
    Load a DataFrame from a CSV file if it exists; otherwise, return an empty DataFrame.
    """
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return pd.DataFrame(columns=['domain'])

def save_dataframe(df, file_path):
    """
    Save the DataFrame to a CSV file.
    """
    df.to_csv(file_path, index=False)

def read_list_from_file(file_path):
    """
    Read lines from a text file and return a list, stripping whitespace.
    """
    try:
        with open(file_path, 'r') as file:
            # Correctly read the file contents and split into lines
            return [line.strip() for line in file.read().splitlines() if line.strip()]
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        print(f"Error: The file {file_path} does not exist.")
        return []

def concatenate_domains(strings, extensions):
    """
    Concatenate each string with a dot and each extension to form full domain names.
    """
    return [f"{s}.{ext}" for s in strings for ext in extensions]

def is_domain_available(domain, attempt=1, max_attempts=5):
    """
    Check if a domain is available using the whois library.
    Implements exponential backoff on failure.
    """
    try:
        w = whois.whois(domain)
        # Handle different formats of 'domain_name'
        if isinstance(w.domain_name, list):
            return not any(w.domain_name)
        return w.domain_name is None or w.domain_name == ''
    except whois.parser.PywhoisError:
        # Domain is available if a PywhoisError is raised
        return True
    except Exception as e:
        if attempt <= max_attempts:
            wait_time = 2 ** attempt
            logging.warning(f"Error checking {domain}: {e}. Retrying in {wait_time} seconds (Attempt {attempt}/{max_attempts}).")
            print(f"Warning: Error checking {domain}. Retrying in {wait_time} seconds (Attempt {attempt}/{max_attempts}).")
            time.sleep(wait_time)
            return is_domain_available(domain, attempt + 1, max_attempts)
        else:
            logging.error(f"Failed to check {domain} after {max_attempts} attempts.")
            print(f"Error: Failed to check {domain} after {max_attempts} attempts.")
            return False

def update_dataframes(domain, available_df, unavailable_df):
    """
    Check domain availability and update the appropriate DataFrame.
    """
    if is_domain_available(domain):
        # Use pd.concat instead of deprecated append
        available_df = pd.concat([available_df, pd.DataFrame({'domain': [domain]})], ignore_index=True)
        logging.info(f"Available: {domain}")
        print(f"Available: {domain}")
    else:
        unavailable_df = pd.concat([unavailable_df, pd.DataFrame({'domain': [domain]})], ignore_index=True)
        logging.info(f"Unavailable: {domain}")
        print(f"Unavailable: {domain}")
    return available_df, unavailable_df

def sort_and_save_dataframes(available_df, unavailable_df, available_file, unavailable_file):
    """
    Sort the DataFrames alphabetically by the 'domain' column and save them.
    """
    # Sort DataFrames
    available_df_sorted = available_df.sort_values(by='domain').reset_index(drop=True)
    unavailable_df_sorted = unavailable_df.sort_values(by='domain').reset_index(drop=True)
    
    # Save sorted DataFrames
    save_dataframe(available_df_sorted, available_file)
    save_dataframe(unavailable_df_sorted, unavailable_file)
    
    logging.info("Sorted and saved available_domains.csv and unavailable_domains.csv.")
    print("Sorted and saved available_domains.csv and unavailable_domains.csv.")

def main():
    """
    Main function to process domain availability.
    """
    # Configure logging
    logging.basicConfig(
        filename='data/domain_checker.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Define file paths
    data_folder = 'data'
    available_file = os.path.join(data_folder, 'available_domains.csv')
    unavailable_file = os.path.join(data_folder, 'unavailable_domains.csv')
    domain_names_file = os.path.join(data_folder, 'domain_names.txt')
    extensions_file = os.path.join(data_folder, 'extensions.txt')
    
    # Ensure data folder exists
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        logging.info(f"Created data folder at {data_folder}")
    
    # Load existing DataFrames
    available_df = load_dataframe(available_file)
    unavailable_df = load_dataframe(unavailable_file)
    
    # Read domain names and extensions from files
    domain_names = read_list_from_file(domain_names_file)
    extensions = read_list_from_file(extensions_file)
    
    if not domain_names or not extensions:
        logging.error("Domain names or extensions list is empty. Exiting.")
        print("Error: Domain names or extensions list is empty. Please check the input files.")
        return
    
    # Concatenate to form full domains
    all_domains = concatenate_domains(domain_names, extensions)
    
    # Combine processed domains to avoid repetition
    processed_domains = set(available_df['domain']).union(set(unavailable_df['domain']))
    
    # Filter out already processed domains
    domains_to_check = [d for d in all_domains if d not in processed_domains]
    
    print(f"Total domains to check: {len(domains_to_check)}")
    logging.info(f"Total domains to check: {len(domains_to_check)}")
    
    # Define sleep time in seconds
    sleep_time = 1  # Adjust as needed (e.g., 1 second between requests)
    
    # Iterate and check each domain
    for index, domain in enumerate(domains_to_check, start=1):
        available_df, unavailable_df = update_dataframes(domain, available_df, unavailable_df)
        
        # Save DataFrames after each iteration
        save_dataframe(available_df, available_file)
        save_dataframe(unavailable_df, unavailable_file)
        
        # Log and print progress
        logging.info(f"Checked {index}/{len(domains_to_check)} domains. Sleeping for {sleep_time} second(s).")
        print(f"Checked {index}/{len(domains_to_check)} domains. Sleeping for {sleep_time} second(s).")
        
        # Sleep to respect rate limits
        time.sleep(sleep_time)
    
    # Sort and save the final DataFrames
    sort_and_save_dataframes(available_df, unavailable_df, available_file, unavailable_file)
    print("Domain checking completed and CSV files have been sorted.")

if __name__ == "__main__":
    main()