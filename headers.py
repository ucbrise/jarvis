#!/usr/bin/env python3

import os
import warnings
from tqdm import tqdm
import pandas as pd

from . import global_state
from . import util
from .experiment import State

def setNotebookName(name):
    global_state.nb_name = name

def getExperimentVersions(experimentName):
    versioningDir = State().versioningDirectory
    processed_out = []
    original_dir = os.getcwd()
    os.chdir(versioningDir + '/' + experimentName)
    raw_out = [x for x in util.runProc('git log').split('\n') if x]
    for line in raw_out:
        if len(line) >= 6 and line[0:6] == 'commit':
            processed_out.append(line.split(' ')[1])
    os.chdir(original_dir)
    return processed_out

def diffExperimentVersions(experimentName, v1, v2):
    original_dir = os.getcwd()
    os.chdir(State().versioningDirectory + '/' + experimentName)
    response = util.runProc('git diff ' + v1 + ' ' + v2)

    os.chdir(original_dir)

    response = response.split('\n')

    segmented_response = []
    file_level_dat = []

    for line in response:
        if len(line) >= 4 and line[0:4] == 'diff':
            if file_level_dat:
                segmented_response.append(file_level_dat)
            file_level_dat = []
        file_level_dat.append(line)

    def isCode(file_level_dat):
        intro = file_level_dat[0].split(' ')
        _, _, old, new = intro
        old = old.split('/')[-1]
        new = new.split('/')[-1]
        old_ext = old.split('.')[-1]
        new_ext = new.split('.')[-1]
        return old_ext == 'py' or new_ext == 'py'

    def getName(file_level_dat):
        intro = file_level_dat[0].split(' ')
        _, _, old, new = intro
        old = old.split('/')[-1]
        new = new.split('/')[-1]
        return old + ' --> ' + new

    def getBody(file_level_dat):
        active = False
        output = [getName(file_level_dat), ]
        for line in file_level_dat:
            if len(line) >= 2 and line[0:2] == '@@':
                active = True
            if active:
                output.append(line)
        return output

    filtered_segmented_response = list(filter(lambda x: isCode(x), segmented_response))

    name_body = {}

    for file_level_dat in filtered_segmented_response:
        if getName(file_level_dat) not in name_body:
            name_body[getName(file_level_dat)] = getBody(file_level_dat)

    res = list(name_body.values())

    color_res = []

    for file_level_dat in res:
        c_file_level_dat = []
        for line in file_level_dat:
            if len(line) >= 1 and line[0:1] == '-':
                c_line = "\033[1;31m" + line
                c_file_level_dat.append(c_line)
            elif len(line) >= 1 and line[0:1] == '+':
                c_line = "\033[1;32m" + line
                c_file_level_dat.append(c_line)
            else:
                c_file_level_dat.append("\033[0;30m" + line)
        color_res.append(c_file_level_dat)

    color_res = ['\n'.join(i) for i in color_res]

    for r in color_res:
        print(r)

def versionSummaries(experimentName):
    original_dir = os.getcwd()
    processed_out = []
    os.chdir(State().versioningDirectory + '/' + experimentName)
    for version in tqdm(getExperimentVersions(experimentName)):
        util.runProc('git checkout ' + version)
        df = util.loadArtifact(experimentName + '.pkl')
        processed_out.append((version, df))
    util.runProc('git checkout master')
    os.chdir(original_dir)

    for experiment_pair in processed_out:
        commithash, df = experiment_pair
        df.loc[:, '__commitHash__'] = [commithash for i in range(len(df))]

    processed_out = list(map(lambda x: x[1], processed_out))
    processed_out = pd.concat(processed_out).reset_index(drop=True)

    return processed_out

def checkoutArtifact(experimentName, trialNum, commitHash, fileName):
    original_dir = os.getcwd()
    os.chdir(State().versioningDirectory + '/' + experimentName)
    util.runProc('git checkout ' + commitHash)
    os.chdir(str(trialNum))
    warnings.filterwarnings("ignore")
    res = util.loadArtifact(fileName)
    warnings.filterwarnings("default")
    os.chdir('../')
    util.runProc('git checkout master')
    os.chdir(original_dir)
    return res

"""
Parameters: 

experimentName : String 
outputDir: ____________
    Absolute or relative path to exported data location. 
commitHash : _______________, optional
    Defaults to the current version if None is provided. 
experimentNewName : String, optional
    Assigns a new name to the experiment. Defaults to experimentName if None is provided. 
trialNum : Integer, optional
    If provided, exports a single trial. Default is to export all of the trials. 
trialName : String, optional
    If provided and a trialNum is provided, assigns a new name to the trial. Defaults to previous trial name. 
"""
def export(experimentName, outputDir, filename, commitHash=None, experimentNewName=None, trialNum=None, trialNewName=None):
    #OTHER TODO: Modularize parallelPull so it has a parameter for export or nah? 
    #TODO: Use checkoutArtifact to get relevant artifacts and then parallelPull them? 
    assert isinstance(experimentName, str)
    #TODO: Check - Validity for commitHash - commitHash is a string
    #TODO: Check - If not current commitHash, then do the checkout code below. else don't 
    #TODO: Check - Validity for outputDir 
    if experimentNewName is None:
        experimentNewName = experimentName
    if trialNewName is None and trialNum is not None:
        trialNewName = str(trialNum)
    
    #FIXME: This only support full experiment exports
    #Trial invariant artifacts should be re-used rather than re-computed across trials.
    #TODO: Add functionality for single trial exports
    original_dir = os.getcwd()
    # try:
    print(State().versioningDirectory + '/' + experimentName)
    os.chdir(State().versioningDirectory + '/' + experimentName)
    # except:
    #     print("Experiment not found. Please make sure you have run the experiment.")
    #     input()

    # util.runProc('git checkout ' + commitHash)
    #cleaning up if necessary
    if trialNum is None:
        print(os.getcwd())
        os.chdir("0")
    else:
        util.runProc("rm *")
        for each in os.listdir("."):
            if each == ".git" or each == str(trialNum):
                continue
            else:
                util.runProc("rm -rf " + each)
        util.runProc("touch .lock")
        os.chdir(str(trialNum))

    #added filename temporarily because experimentName != python file name
    util.runProc("python " + filename)

    #TODO: Copy entire folder to outputDir
    #TODO: OR Run new experiment and have output into the jarvis.d subdir then copy over
    #In the case above, we need to save the most recent commitHash so we can reset master
    #code for copying:
    import shutil
    outputDir = os.path.abspath(outputDir)
    #TODO: bug involving deleting existing directory
    if trialNum is None:
        os.chdir("..")
        shutil.copytree(experimentName, outputDir)
        os.chdir(experimentName)
    else:
        shutil.copytree(str(trialNum), outputDir)

    # if no commit hash, use this block to run the code to copy files out then reset state of files
    util.runProc('git reset --hard HEAD')

    # util.runProc('git checkout master')
    # os.chdir(original_dir)

    #TODO: util.runProc('python {}.py'.format(experimentName))
