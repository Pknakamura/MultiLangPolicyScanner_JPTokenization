import pandas as pd
import re
import os
import glob
import sudachipy
import cProfile
import pstats
from pstats import SortKey

# in_folder = 'Raw_Text(Sample)'
# out_folder = 'Processed_Text(Sample)'

#-------- misc helper functions: ---------#

#pre-checks for japanese by looking for hiragana/katakana -- very simple -- good specificity
#posibly improve by checking OOV after tokenization
with open("misc/kana.txt", "r") as f:
    kana = set(f.read())
def simple_japanese_test(text :str ,threshold = 1):
    """hiragana/katakana based language tester

    Parameters
    ----------
    text : text to be checked
    threshold: minimum number of kana to find to pass test

    Returns
    -------
    bool
        True if Japanese, False otherwise
    """

    text_set = set(text)
    check = text_set.intersection(kana)
    return len(check) >= threshold

#Function using table of URL <-> Hash for mapping file names to correct website
def file_matcher_str(url_table : pd.DataFrame, hash : str):
    """uses table to match websites to their hash

    Parameters
    ----------
    url_table :dataframe with 2 rows, 'url' and 'hash'
    hash: the coresponidng url hash

    Returns
    -------
    str
        If found: URL
        If not found: "{hash} has no associated URL"
        If found more than once: "{hash} has too many associated URL's"
    """
    index = url_table[url_table['hash'] == hash].index.to_list()

    #check if text has an associated URL
    if(len(index) < 1):
        return f"{hash} has no associated URL"
    if(len(index) > 1):
        return f"{hash} has too many associated URL's"
    
    return url_table[index,"url"]

#Function using table of URL <-> Hash to confirm hash has valid url
def file_matcher_bool(url_table : pd.DataFrame, hash : str):
    """uses table to check for url

    Parameters
    ----------
    url_table :dataframe with 2 rows, 'url' and 'hash'
    hash: the coresponidng url hash

    Returns
    -------
    bool
        If in table exactly once: True
        Else: False
    """
    index = url_table[url_table['hash'] == hash].index.to_list()

    #check if text has an associated URL
    if(len(index) == 1):
        return True
    
    return False

#helper function to collect POS tags for POS translation 
#only needs to be used once
def get_pos_tags(text : str, tokenizer : sudachipy.Tokenizer):
    """Takes a text file and tokenizes it, keeping track of every unique POS tag

    Parameters
    ----------
    text: The text to find POS tags of
    tokenizer: SudahiPy tokenizer to use for tokenization

    Returns
    -------
    set
        Contains every found POS once
    """

    to_return = set()
    
    #clean and split text by "section"
    text = text.strip()
    text = re.sub('[\n\r]+','\n',text)
    sections = text.split("\n")
    
    #tokenize section by section to avoid SudachiPy input size limit
    for sec in sections:
        tokens = tokenizer.tokenize(sec)

        #loops over tokens - each token is a new row
        for token in tokens:
            pos = token.part_of_speech()
            for p in pos:
                to_return.add(p)

    return to_return

#-------- tokenization helper functions: ---------#

#tokenization helper function
def tokenize(f_name : str, text : str, tokenizer : sudachipy.Tokenizer, stopwords : set = {}):
    """Tokenization helper function

    Parameters
    ----------
    f_name : Name of output file relative to current directory, in this case often a hash
    text : The text to tokenize
    tokenizer : The SudachiPy tokenizer to use
    stopwords : Set of stopwords to remove (Usu. stopwords-ja.txt)

    Returns
    -------
    void
        Creates file at the file location specified
    """

    #clean and split text by "section"
    text = text.strip()
    text = re.sub('[\n\r]+','\n',text)
    text = re.sub(',|，','、',text)#replace our seperator value with japanese equvalent
    sections = text.split("\n")
    
    with open(f"{f_name}.csv", "w+") as out:
        #headers
        out.write(f"Surface,Normalized,Reading,Dictionary,POS1,POS2,POS3,POS4,Conj_Type,Conj,OOV,B_Split,A_Split,Section\n")

        #tokenize section by section to avoid SudachiPy input size limit
        for idx, sec in enumerate(sections):
            tokens = tokenizer.tokenize(sec)

            #loops over tokens - each token is a new row
            for token in tokens:

                if(token.normalized_form() in stopwords):
                    continue

                #split POS tuple - always 6 elements
                pos = token.part_of_speech()

                if(pos[0] == "空白"):
                    continue
                if(pos[0] == "補助記号"):
                    if(pos[1] != "句点" and pos[1] != "読点" ):
                        continue
                
                #spliting on splitmode A
                a_split = [sub_token.surface() for sub_token in token.split(sudachipy.SplitMode.A, add_single = False)] #split tuple of morphemes into list of strings
                if len(a_split) == 0:
                    a_split = "*" #matches SudachiPy na value
                else:
                    a_split = "-".join(a_split)

                #spliting on splitmode B
                b_split = [sub_token.surface() for sub_token in token.split(sudachipy.SplitMode.B, add_single = False)] #split tuple of morphemes into list of strings
                if len(b_split) == 0:
                    b_split = "*" #matches SudachiPy na value
                else:
                    b_split = "-".join(b_split) 

                #write tokens to f_name.csv
                out.write(f"{token.surface()},{token.normalized_form()},{token.reading_form()},{token.dictionary_form()},{",".join(pos)},{token.is_oov()},{b_split},{a_split},{idx+1}\n")

#tokenizes all files in given folder
def mass_tokenizer(in_folder : str, out_folder : str, stopwords : set = {}):
    """Takes all text files from imput folder and tokenized them as csv files in output foulder 

    Parameters
    ----------
    in_folder : relative path to input folder
    out_folder : relative path to out put folder
    stopwords : Set of stopwords to remove (Usu. stopwords-ja.txt)

    Returns
    -------
    void
        Creates tokenized csv files in out_folder
    """
    
    #create dictionary - Using full for best performance
    full_dict = sudachipy.Dictionary(dict = "full")
    #create tokenizer - Spliting on highest level for NER
    tokenizer_C = full_dict.create(mode = sudachipy.SplitMode.C)

    # unique_pos = set()

    for file in glob.glob(os.path.join(in_folder, '*.txt')):
        filename = os.path.splitext(os.path.basename(file))[0]
        
        with open(file, 'r') as f:
            text = f.read()

            if(simple_japanese_test(text)):
                tokenize(f'{out_folder}/{filename}', text, tokenizer_C, stopwords = stopwords) #creates and writes tokenized csv of the given text file
                #unique_pos = unique_pos.union(get_pos_tags(text, tokenizer_C))

                # print(filename)
                # print(simple_japanese_test(text))
                # test = pd.read_csv(f"{out_folder}/{filename}.csv", sep = ",")
                # print(oov_japanese_test(test))
                # print("-------------------------")

    # with open("misc/POS_set", "w+") as file2: 
    #     file2.write(",\n".join(unique_pos))

    full_dict.close()


#-------- Running our tokenizer --------#

#test speed/see what is slowest
pr = cProfile.Profile()
pr.enable()

#get stopwords from stopwords-ja.txt
with open("misc/stopwords-ja.txt", "r") as f:
    stopwords = set(f.read().split("\n"))
print(len(stopwords))

mass_tokenizer("Raw_Text(Sample)","Processed_Text(Sample)", stopwords = stopwords)

pr.disable()
pr.dump_stats('misc/stats')
p = pstats.Stats('misc/stats')
p.strip_dirs().sort_stats(SortKey.TIME).print_stats(20)

#-------- Main pre-analysis: --------#
# - Verifying the language of policy(maybe language translation?)
# - Character encoding for non english language/characters(for storing and processing)
# - Word and Sentence frequency.
# - Average Sentence, paragraph and document length.
# - Paragraph segmentation(identify distinct sections within policy)
# - Named Entity Recognition for non english textual documents