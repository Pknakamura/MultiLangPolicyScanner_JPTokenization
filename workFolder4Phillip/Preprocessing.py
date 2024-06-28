import pandas as pd
import re
import os
import glob
import sudachipy
import cProfile
import pstats
from pstats import SortKey

in_folder = 'Raw_Text(Sample)'
out_folder = 'Processed_Text(Sample)'

#-------- helper functions: ---------#

#pre-checks for japanese by looking for hiragana/katakana -- very simple -- good specificity
#posibly improve by checking OOV after tokenization
hiragana = set("ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんゔゝゞ")
katakana = set("ァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶヷヸヹヺヽヾ")
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
    check = text_set.intersection(hiragana.union(katakana))
    return len(check) >= threshold


#tokenization helper function
def tokenize(f_name : str, text : str, tokenizer : sudachipy.Tokenizer, stopwording = False, stopwording_keep : list = None):
    """Tokenization helper function

    Parameters
    ----------
    f_name : Name of output file relative to current directory, in this case often a hash.
    text : The text to tokenize.
    tokenizer : The SudachiPy tokenizer to use.
    stopwording : Whether or not to perform stop wording.
    stopwording_keep : Things to omit from stopwording process
        ex: ['All_Punc', 'All_Spaces', '。','、','　', #any word in https://github.com/stopwords-iso/stopwords-ja]

    Returns
    -------
    void
        Creates file at the file location specified
    """

    #clean and split text by "section"
    text = text.strip()
    text = re.sub('[\n\r]+','\n',text)
    sections = text.split("\n")
    
    with open(f"{f_name}.csv", "w+") as out:
        #headers
        out.write(f"Surface,Normalized,Reading,Dictionary,POS1,POS2,POS3,POS4,Conj_Type,Conj,OOV,B_Split,A_Split,Section\n")

        #tokenize section by section to avoid SudachiPy input size limit
        for idx, sec in enumerate(sections):
            tokens = tokenizer.tokenize(sec)

            #loops over tokens - each token is a new row
            for token in tokens:
                #split POS tuple - always 6 elements
                POS = ",".join(token.part_of_speech())
                
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
                out.write(f"{token.surface()},{token.normalized_form()},{token.reading_form()},{token.dictionary_form()},{POS},{token.is_oov()},{b_split},{a_split},{idx+1}\n")


#-------- tokenization: ---------#
#table of URL <-> Hash for mapping file names to correct website
url_table = pd.read_csv(f"{in_folder}/sample_policies_japanese.csv",sep = ",")

#create dictionary - Using full for best performance
full_dict = sudachipy.Dictionary(dict = "full")
#create tokenizer - Spliting on highest level for NER
tokenizer_C = full_dict.create(mode = sudachipy.SplitMode.C)

pr = cProfile.Profile()
pr.enable()
#opening files
for filename in glob.glob(os.path.join(in_folder, '*.txt')):
    filehash = os.path.splitext(os.path.basename(filename))[0]
    index = url_table[url_table['hash'] == filehash].index.to_list()
    
    #check if text has an associated URL
    if(len(index) < 1):
        #print(f"{filename} has no associated URL")
        continue
    if(len(index) > 1):
        #print(f"{filename} has too many associated URL's")
        continue
    
    with open(filename, 'r') as f:
        tokenize(f'{out_folder}/{filehash}', f.read(), tokenizer_C) #creates and writes tokenized csv of the given text file

pr.disable()
pr.dump_stats('stats')
p = pstats.Stats('stats')
p.strip_dirs().sort_stats(SortKey.TIME).print_stats(20)


#-------- Main pre-analysis: --------#
# - Verifying the language of policy(maybe language translation?)
# - Character encoding for non english language/characters(for storing and processing)
# - Word and Sentence frequency.
# - Average Sentence, paragraph and document length.
# - Paragraph segmentation(identify distinct sections within policy)
# - Named Entity Recognition for non english textual documents








