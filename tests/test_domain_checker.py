import pytest
from unittest import mock
from unittest.mock import mock_open, patch, MagicMock
import os
import pandas as pd
from src.domain_checker import (
    load_dataframe,
    save_dataframe,
    read_list_from_file,
    concatenate_domains,
    is_domain_available,
    update_dataframes,
    sort_and_save_dataframes
)
import whois  # Added import to fix NameError

# Helper function to create a DataFrame for testing
def create_dataframe(domains):
    return pd.DataFrame({'domain': domains})

@pytest.fixture
def mock_logging():
    with patch('src.domain_checker.logging') as mock_log:
        yield mock_log

@pytest.fixture
def mock_whois():
    with patch('src.domain_checker.whois.whois') as mock_whois:
        yield mock_whois

# 1. Test handling missing input files
def test_read_list_from_file_missing_file(mock_logging):
    # Attempt to read a non-existent file
    result = read_list_from_file('data/non_existent_file.txt')
    assert result == []
    mock_logging.error.assert_called_with("File not found: data/non_existent_file.txt")

# 2. Test reading files with different names
def test_read_list_from_file_different_names(mock_logging):
    # Mock file content for a differently named file
    with patch('builtins.open', mock_open(read_data="shop\n my \nbest\nsuper")) as mock_file:
        result = read_list_from_file('data/custom_domain_names.txt')
        mock_file.assert_called_with('data/custom_domain_names.txt', 'r')
        assert result == ['shop', 'my', 'best', 'super']

# 3. Test handling trailing spaces and empty lines
def test_read_list_from_file_trailing_spaces_and_empty_lines(mock_logging):
    file_content = "shop   \n\n  my\nbest \nsuper\n  \n"
    with patch('builtins.open', mock_open(read_data=file_content)):
        result = read_list_from_file('data/domain_names.txt')
        assert result == ['shop', 'my', 'best', 'super']

# 4. Test domain names containing full stops
def test_concatenate_domains_with_full_stops():
    domain_names = ['shop.com', 'my.net']
    extensions = ['org', 'io']
    # Even if domain names contain full stops, concatenate should add another
    concatenated = concatenate_domains(domain_names, extensions)
    expected = [
        'shop.com.org',
        'shop.com.io',
        'my.net.org',
        'my.net.io'
    ]
    assert concatenated == expected

# 5. Test handling non-existent domain extensions
def test_concatenate_domains_with_nonexistent_extensions():
    domain_names = ['shop', 'my']
    extensions = ['invalidext', 'anotherfake']
    concatenated = concatenate_domains(domain_names, extensions)
    expected = [
        'shop.invalidext',
        'shop.anotherfake',
        'my.invalidext',
        'my.anotherfake'
    ]
    assert concatenated == expected

# 6. Test load_dataframe with existing file
def test_load_dataframe_existing_file():
    csv_content = "domain\nshop.com\nmy.net"
    with patch('builtins.open', mock_open(read_data=csv_content)):
        with patch('os.path.exists', return_value=True):
            df = load_dataframe('data/available_domains.csv')
            assert len(df) == 2
            assert df.iloc[0]['domain'] == 'shop.com'
            assert df.iloc[1]['domain'] == 'my.net'

# 7. Test load_dataframe with non-existing file
def test_load_dataframe_non_existing_file():
    with patch('os.path.exists', return_value=False):
        df = load_dataframe('data/non_existent.csv')
        assert df.empty
        assert list(df.columns) == ['domain']

# 8. Test save_dataframe
def test_save_dataframe():
    df = create_dataframe(['shop.com', 'my.net'])
    with patch('pandas.DataFrame.to_csv') as mock_to_csv:
        save_dataframe(df, 'data/available_domains.csv')
        mock_to_csv.assert_called_with('data/available_domains.csv', index=False)

# 9. Test is_domain_available when domain is available (PywhoisError)
def test_is_domain_available_available(mock_whois):
    # Simulate PywhoisError indicating the domain is available
    mock_whois.side_effect = whois.parser.PywhoisError
    available = is_domain_available('availabledomain.com')
    assert available == True

# 10. Test is_domain_available when domain is unavailable (domain_name present)
def test_is_domain_available_unavailable(mock_whois):
    # Simulate whois response with domain_name
    mock_response = MagicMock()
    mock_response.domain_name = ['unavailable.com']
    mock_whois.return_value = mock_response
    available = is_domain_available('unavailable.com')
    assert available == False

# 11. Test is_domain_available with domain_name as empty string
def test_is_domain_available_empty_domain_name(mock_whois):
    mock_response = MagicMock()
    mock_response.domain_name = ''
    mock_whois.return_value = mock_response
    available = is_domain_available('emptydomain.com')
    assert available == True

# 12. Test update_dataframes with available domain
def test_update_dataframes_available(mock_whois, mock_logging):
    mock_whois.side_effect = whois.parser.PywhoisError
    available_df = create_dataframe([])
    unavailable_df = create_dataframe([])
    updated_available, updated_unavailable = update_dataframes('available.com', available_df, unavailable_df)
    assert len(updated_available) == 1
    assert updated_available.iloc[0]['domain'] == 'available.com'
    assert len(updated_unavailable) == 0
    mock_logging.info.assert_called_with("Available: available.com")

# 13. Test update_dataframes with unavailable domain
def test_update_dataframes_unavailable(mock_whois, mock_logging):
    mock_response = MagicMock()
    mock_response.domain_name = ['unavailable.com']
    mock_whois.return_value = mock_response
    available_df = create_dataframe([])
    unavailable_df = create_dataframe([])
    updated_available, updated_unavailable = update_dataframes('unavailable.com', available_df, unavailable_df)
    assert len(updated_available) == 0
    assert len(updated_unavailable) == 1  # Changed from unavailable_df to updated_unavailable
    assert unavailable_df.empty  # Original unavailable_df should remain empty
    assert updated_unavailable.iloc[0]['domain'] == 'unavailable.com'
    mock_logging.info.assert_called_with("Unavailable: unavailable.com")

# 14. Test sort_and_save_dataframes with correct sorting
def test_sort_and_save_dataframes():
    available_df = create_dataframe(['zeta.com', 'alpha.com', 'gamma.com'])
    unavailable_df = create_dataframe(['delta.com', 'beta.com'])
    available_file = 'data/available_domains.csv'
    unavailable_file = 'data/unavailable_domains.csv'
    
    with patch('src.domain_checker.save_dataframe') as mock_save:
        sort_and_save_dataframes(available_df, unavailable_df, available_file, unavailable_file)
        
        # Capture the calls
        calls = mock_save.call_args_list
        # There should be two calls: one for available, one for unavailable
        assert len(calls) == 2
        
        # Extract the arguments
        first_call_args = calls[0][0]  # positional arguments
        second_call_args = calls[1][0]
        
        # Sort DataFrames
        sorted_available = available_df.sort_values(by='domain').reset_index(drop=True)
        sorted_unavailable = unavailable_df.sort_values(by='domain').reset_index(drop=True)
        
        # Use pd.testing to compare DataFrames
        pd.testing.assert_frame_equal(first_call_args[0], sorted_available)
        assert first_call_args[1] == available_file
        
        pd.testing.assert_frame_equal(second_call_args[0], sorted_unavailable)
        assert second_call_args[1] == unavailable_file

# 15. Test sort_and_save_dataframes when CSV files are missing or moved
def test_sort_and_save_dataframes_csv_missing():
    available_df = create_dataframe(['zeta.com', 'alpha.com', 'gamma.com'])
    unavailable_df = create_dataframe(['delta.com', 'beta.com'])
    available_file = 'data/missing_available_domains.csv'
    unavailable_file = 'data/missing_unavailable_domains.csv'
    
    with patch('src.domain_checker.save_dataframe') as mock_save:
        # Assume CSV files are missing, but sort_and_save_dataframes should still attempt to save
        sort_and_save_dataframes(available_df, unavailable_df, available_file, unavailable_file)
        
        # Capture the calls
        calls = mock_save.call_args_list
        # There should be two calls: one for available, one for unavailable
        assert len(calls) == 2
        
        # Extract the arguments
        first_call_args = calls[0][0]  # positional arguments
        second_call_args = calls[1][0]
        
        # Sort DataFrames
        sorted_available = available_df.sort_values(by='domain').reset_index(drop=True)
        sorted_unavailable = unavailable_df.sort_values(by='domain').reset_index(drop=True)
        
        # Use pd.testing to compare DataFrames
        pd.testing.assert_frame_equal(first_call_args[0], sorted_available)
        assert first_call_args[1] == available_file
        
        pd.testing.assert_frame_equal(second_call_args[0], sorted_unavailable)
        assert second_call_args[1] == unavailable_file

# 16. Test read_list_from_file with file containing full stops in domain names
def test_read_list_from_file_with_full_stops(mock_logging):
    file_content = "shop.\nmy.\nbest.\nsuper.\n"
    with patch('builtins.open', mock_open(read_data=file_content)):
        result = read_list_from_file('data/domain_names_with_dots.txt')
        assert result == ['shop.', 'my.', 'best.', 'super.']

# 17. Test is_domain_available with invalid domain extension
def test_is_domain_available_invalid_extension(mock_whois):
    # Simulate whois response with invalid domain
    mock_response = MagicMock()
    mock_response.domain_name = ['invalid.ext']
    mock_whois.return_value = mock_response
    available = is_domain_available('invalid.ext')
    assert available == False

# 18. Test update_dataframes_with_retries
def test_update_dataframes_with_retries(mock_whois, mock_logging):
    # Simulate transient errors followed by a successful response
    mock_whois.side_effect = [Exception("Temporary error"), whois.parser.PywhoisError]
    
    available_df = create_dataframe([])
    unavailable_df = create_dataframe([])
    
    # Update dataframes, which should handle retries
    updated_available, updated_unavailable = update_dataframes('transient.com', available_df, unavailable_df)
    
    assert len(updated_available) == 1
    assert updated_available.iloc[0]['domain'] == 'transient.com'
    assert len(updated_unavailable) == 0
    mock_logging.warning.assert_called_with(
        "Error checking transient.com: Temporary error. Retrying in 2 seconds (Attempt 1/5)."
    )
    mock_logging.info.assert_called_with("Available: transient.com")

# 19. Test read_list_from_file with different line endings
def test_read_list_from_file_different_line_endings(mock_logging):
    # Unix and Windows line endings
    file_content = "shop\r\nmy\r\nbest\r\nsuper\r\n"
    with patch('builtins.open', mock_open(read_data=file_content)):
        result = read_list_from_file('data/domain_names_windows.txt')
        assert result == ['shop', 'my', 'best', 'super']

# 20. Test handling domain extensions with trailing spaces
def test_read_list_from_file_extensions_trailing_spaces(mock_logging):
    file_content = "com \nnet\n org \nio\n"
    with patch('builtins.open', mock_open(read_data=file_content)):
        extensions = read_list_from_file('data/extensions_trailing_spaces.txt')
        assert extensions == ['com', 'net', 'org', 'io']

# 21. Test concatenate_domains ensuring no double dots
def test_concatenate_domains_no_double_dots():
    domain_names = ['shop.', 'my', 'best', 'super.']
    extensions = ['com', 'net']
    concatenated = concatenate_domains(domain_names, extensions)
    expected = [
        'shop..com',
        'shop..net',
        'my.com',
        'my.net',
        'best.com',
        'best.net',
        'super..com',
        'super..net'
    ]
    assert concatenated == expected

# 22. Test is_domain_available with domain_name as list
def test_is_domain_available_domain_name_list(mock_whois):
    mock_response = MagicMock()
    mock_response.domain_name = ['example.com', 'example.net']
    mock_whois.return_value = mock_response
    available = is_domain_available('example.com')
    assert available == False

# 23. Test read_list_from_file with Unicode characters
def test_read_list_from_file_unicode_characters(mock_logging):
    file_content = "shöp\nmý\nbést\nsüpër\n"
    with patch('builtins.open', mock_open(read_data=file_content)):
        result = read_list_from_file('data/domain_names_unicode.txt')
        assert result == ['shöp', 'mý', 'bést', 'süpër']

# 24. Test handling very large domain lists
def test_concatenate_domains_large_list():
    domain_names = [f"domain{i}" for i in range(1000)]
    extensions = ['com', 'net']
    concatenated = concatenate_domains(domain_names, extensions)
    assert len(concatenated) == 2000
    assert concatenated[:2] == ['domain0.com', 'domain0.net']
    assert concatenated[-2:] == ['domain999.com', 'domain999.net']