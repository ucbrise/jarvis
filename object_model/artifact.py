#!/usr/bin/env python3

import os
import git
import json
import itertools
import datetime
import pandas as pd
import sys

from graphviz import Digraph
from shutil import copyfile
from shutil import rmtree
from shutil import copytree
from shutil import move

from ray.tune import register_trainable
from ray.tune import grid_search
from ray.tune import run_experiments

from .. import util
from .recursivesizeof import total_size

import ray

class Artifact:

    def __init__(self, loc, parent, manifest, xp_state):
        self.loc = loc
        self.parent = parent

        if self.parent:
            self.parent.out_artifacts.append(self)

        self.xp_state = xp_state

    def __commit__(self):

        gc = self.xp_state.gc #ground client
        dir_name = self.xp_state.versioningDirectory
        loclist = self.loclist
        scriptNames = self.scriptNames
        tag = {
            'Artifacts': [i for i in loclist], #maps "artifacts" to paths to artifacts
            'Actions': [i for i in scriptNames] #similar but with "actions"
        }

        for literal in self.xp_state.literals:
            if literal.name:
                try:
                    value = str(util.unpickle(literal.loc))
                    if len(value) <= 250:
                        tag[literal.name] = value #maps literal names to literal values
                except:
                    pass

        if not os.path.exists(dir_name):
            nodeid = gc.createNode('Run')
            gc.createNodeVersion(nodeid, tag) #ground initialization

            os.makedirs(dir_name) #create directory
            #FIXME: need to create experiment name
            os.makedirs(dir_name + '/1') #creating subdirectory for experiment numbers
            # Move new files to the artifacts repo
            for loc in loclist:
                copyfile(loc, dir_name + "/1/" + loc)
            for script in scriptNames:
                copyfile(script, dir_name + "/1/" + script)
            os.chdir(dir_name + '/1')

            gc.commit() #move everything into the first subdirectory and commits it
            os.chdir('../') #operating in jarvis.d

            repo = git.Repo.init(os.getcwd()) #initialize git repo
            repo.index.add(['1',])

            repo.index.commit("initial commit")
            tree = repo.tree()
            with open('.jarvis', 'w') as f:
                for obj in tree:
                    commithash = util.runProc("git log " + obj.path).replace('\n', ' ').split()[1]
                    if obj.path != '.jarvis':
                        f.write(obj.path + " " + commithash + "\n")
            repo.index.add(['.jarvis'])
            repo.index.commit('.jarvis commit')
            os.chdir('../') #creates .jarvis if not one, otherwise overwrites and commits.
        else:

            listdir = [x for x in filter(util.isNumber, os.listdir(dir_name))] #makes not of how many numbers

            #FIXME: rerrunning the experiment should overrite files not create all new files.
            nthDir =  str(len(listdir) + 1)
            os.makedirs(dir_name + "/" + nthDir)
            for loc in loclist:
                copyfile(loc, dir_name + "/" + nthDir + "/" + loc)
            for script in scriptNames:
                copyfile(script, dir_name + "/" + nthDir + "/" + script)
            os.chdir(dir_name + "/" + nthDir) #adding an extra directory

            gc.load()

            run_node = gc.getNode('Run')
            #FIXME: below two lines may not be necessary - could replace w/ parents = None only?
            parents = []

            if not parents:
                parents = None
            gc.createNodeVersion(run_node.nodeId, tag, parents) #question..the function returns dictionary d, but the return value is not used or saved?
            #ask rolando about this. There are two createNodeVersion

            gc.commit()

            os.chdir('../')
            repo = git.Repo(os.getcwd())

            repo.index.add([nthDir,])

            repo.index.commit("incremental commit")
            tree = repo.tree()
            with open('.jarvis', 'w') as f:
                for obj in tree:
                    commithash = util.runProc("git log " + obj.path).replace('\n', ' ').split()[1]
                    if obj.path != '.jarvis':
                        f.write(obj.path + " " + commithash + "\n")
            repo.index.add(['.jarvis'])
            repo.index.commit('.jarvis commit')
            os.chdir('../')

    def __pull__(self):
        """
        Partially refactored
        :return:
        """
        self.xp_state.visited = []
        driverfile = self.xp_state.jarvisFile

        if not util.isOrphan(self):
            self.loclist = list(map(lambda x: x.getLocation(), self.parent.out_artifacts))
        else:
            self.loclist = [self.getLocation(),]
        self.scriptNames = []
        if not util.isOrphan(self):
            self.parent.__run__(self.loclist, self.scriptNames)
        self.loclist = list(set(self.loclist))
        self.scriptNames = list(set(self.scriptNames))


        # Need to sort to compare
        self.loclist.sort()
        self.scriptNames.sort()
     
    # def parallelPull(self, manifest={}):
    #
    #     self.xp_state.versioningDirectory = os.path.expanduser('~') + '/' + 'jarvis.d'
    #
    #     # Runs one experiment per pull
    #     # Each experiment has many trials
    #
    #     tmpexperiment = self.xp_state.tmpexperiment
    #     if os.path.exists(tmpexperiment):
    #         rmtree(tmpexperiment)
    #         os.mkdir(tmpexperiment)
    #     else:
    #         os.mkdir(tmpexperiment)
    #
    #     self.xp_state.visited = []
    #
    #     if not util.isOrphan(self):
    #         self.loclist = list(map(lambda x: x.getLocation(), self.parent.out_artifacts)) #get location of all out_artifacts
    #     else:
    #         self.loclist = [self.getLocation(),]
    #     self.scriptNames = []
    #
    #     literalsAttached = set([])
    #     lambdas = [] #Encompasses actions that need to be done
    #     if not util.isOrphan(self):
    #         self.parent.__serialize__(lambdas, self.loclist, self.scriptNames) #?
    #
    #     self.loclist = list(set(self.loclist))
    #     self.scriptNames = list(set(self.scriptNames))
    #
    #     # Need to sort to compare
    #     self.loclist.sort()
    #     self.scriptNames.sort()
    #
    #     for _, names in lambdas:
    #         literalsAttached |= set(names)
    #
    #     original_dir = os.getcwd()
    #
    #     experimentName = self.xp_state.jarvisFile.split('.')[0]
    #     #FIXME: May not be necessary in Pure Ray
    #     def exportedExec(config, reporter):
    #         tee = tuple([])
    #         for litName in config['8ilk9274']:
    #             tee += (config[litName], )
    #         i = -1
    #         for j, v in enumerate(config['6zax7937']):
    #             if v == tee:
    #                 i = j
    #                 break
    #         assert i >= 0
    #         os.chdir(tmpexperiment + '/' + str(i))
    #         with open('.' + experimentName + '.jarvis', 'w') as fp:
    #             json.dump(config, fp)
    #         #This is necessary for the Pure Ray implementation.
    #         print(lambdas)
    #         for f, names in lambdas:
    #             print(names)
    #             literals = list(map(lambda x: config[x], names))
    #             print(literals)
    #             f(literals)
    #         input()
    #         reporter(timesteps_total=1)
    #         os.chdir(original_dir)
    #
    #     #Tune
    #     config = {}
    #     numTrials = 1
    #     literals = []
    #     literalNames = []
    #     for kee in self.xp_state.literalNameToObj:
    #         if kee in literalsAttached:
    #             if self.xp_state.literalNameToObj[kee].__oneByOne__:
    #
    #                 #Tune
    #                 config[kee] = grid_search(self.xp_state.literalNameToObj[kee].v)
    #                 numTrials *= len(self.xp_state.literalNameToObj[kee].v)
    #                 literals.append(self.xp_state.literalNameToObj[kee].v)
    #             else:
    #                 #Tune
    #                 config[kee] = self.xp_state.literalNameToObj[kee].v
    #                 if util.isIterable(self.xp_state.literalNameToObj[kee].v):
    #                     if type(self.xp_state.literalNameToObj[kee].v) == tuple:
    #                         literals.append((self.xp_state.literalNameToObj[kee].v, ))
    #                     else:
    #                         literals.append([self.xp_state.literalNameToObj[kee].v, ])
    #                 else:
    #                     literals.append([self.xp_state.literalNameToObj[kee].v, ])
    #             literalNames.append(kee)
    #
    #     literals = list(itertools.product(*literals))
    #     #Tune
    #     config['6zax7937'] = literals
    #     #Tune
    #     config['8ilk9274'] = literalNames
    #
    #     for i in range(numTrials):
    #         dst = tmpexperiment + '/' + str(i)
    #         copytree(os.getcwd(), dst, True)
    #
    #
    #     ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    #
    #     register_trainable('exportedExec', exportedExec)
    #
    #
    #
    #     self.xp_state.ray['literalNames'] = literalNames
    #
    #     #Tune
    #     #FIXME: run_experiments is equivalent of calling remote fn multiple times
    #     run_experiments({
    #         experimentName : {
    #             'run': 'exportedExec',
    #             'resources': {'cpu': 1, 'gpu': 0},
    #             'config': config
    #         }
    #     })
    #
    #     if not os.path.isdir(self.xp_state.versioningDirectory):
    #         os.mkdir(self.xp_state.versioningDirectory)
    #
    #     moveBackFlag = False
    #
    #     if os.path.exists(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0]):
    #         move(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0] + '/.git', '/tmp/')
    #         rmtree(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
    #         moveBackFlag = True
    #
    #     if manifest:
    #
    #         os.chdir(tmpexperiment)
    #
    #         dirs = [x for x in os.listdir() if util.isNumber(x)]
    #         table_full = []
    #         table_small = []
    #
    #         for trial in dirs:
    #             os.chdir(trial)
    #             with open('.' + experimentName + '.jarvis', 'r') as fp:
    #                 config = json.load(fp)
    #             record_full = {}
    #             record_small = {}
    #
    #             for literalName in literalNames:
    #                 record_full[literalName] = config[literalName]
    #                 record_small[literalName] = config[literalName]
    #             for artifactLabel in manifest:
    #                 record_full[artifactLabel] = util.loadArtifact(manifest[artifactLabel].loc)
    #                 if total_size(record_full[artifactLabel]) >= 1000:
    #                     record_small[artifactLabel] = " . . . "
    #                 else:
    #                     record_small[artifactLabel] = record_full[artifactLabel]
    #                 if util.isNumber(record_full[artifactLabel]):
    #                     record_full[artifactLabel] = eval(record_full[artifactLabel])
    #                 if util.isNumber(record_small[artifactLabel]):
    #                     record_small[artifactLabel] = eval(record_small[artifactLabel])
    #             record_small['__trialNum__'] = trial
    #             record_full['__trialNum__'] = trial
    #
    #             table_full.append(record_full)
    #             table_small.append(record_small)
    #             os.chdir('../')
    #
    #         df = pd.DataFrame(table_small)
    #         util.pickleTo(df, experimentName + '.pkl')
    #
    #         os.chdir(original_dir)
    #
    #     copytree(tmpexperiment, self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
    #
    #     os.chdir(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
    #     if moveBackFlag:
    #         move('/tmp/.git', self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
    #         repo = git.Repo(os.getcwd())
    #         repo.git.add(A=True)
    #         repo.index.commit('incremental commit')
    #     else:
    #         repo = git.Repo.init(os.getcwd())
    #         repo.git.add(A=True)
    #         repo.index.commit('initial commit')
    #     os.chdir(original_dir)
    #
    #     if manifest:
    #
    #         return pd.DataFrame(table_full)

    #new version of parallelPull
    def parallelPull(self, manifest={}):

        self.xp_state.versioningDirectory = os.path.expanduser('~') + '/' + 'jarvis.d'

        tmpexperiment = self.xp_state.tmpexperiment
        if os.path.exists(tmpexperiment):
            rmtree(tmpexperiment)
        os.mkdir(tmpexperiment)

        self.xp_state.visited = []

        if not util.isOrphan(self):
            self.loclist = list(map(lambda x: x.getLocation(), self.parent.out_artifacts))
        else:
            self.loclist = [self.getLocation(), ]
        self.scriptNames = []

        literalsAttached = set([])
        lambdas = []

        if not util.isOrphan(self):
            self.parent.__serialize__(lambdas, self.loclist, self.scriptNames)

        self.loclist = list(set(self.loclist))
        self.scriptNames = list(set(self.scriptNames)) # needed

        self.loclist.sort()
        self.scriptNames.sort()  # needed

        for _, names in lambdas:
            literalsAttached |= set(names)

        original_dir = os.getcwd()
        experimentName = self.xp_state.jarvisFile.split('.')[0]  # same until after here

        numTrials = 1
        literals = []
        literalNames = []

        for kee in self.xp_state.literalNameToObj:
            if kee in literalsAttached:
                if self.xp_state.literalNameToObj[kee].__oneByOne__:
                    numTrials *= len(self.xp_state.literalNameToObj[kee].v)
                    literals.append(self.xp_state.literalNameToObj[kee].v)
                else:
                    if type(self.xp_state.literalNameToObj[kee].v) == tuple:
                        literals.append((self.xp_state.literalNameToObj[kee].v,))
                    else:
                        literals.append([self.xp_state.literalNameToObj[kee].v, ])
                literalNames.append(kee)

        literals = list(itertools.product(*literals))
        print(literals)

        for i in range(numTrials):
            dst = tmpexperiment + '/' + str(i)
            copytree(os.getcwd(), dst, True)  # TODO: Check if needed

        # ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')  #probably not needed
        self.xp_state.ray['literalNames'] = literalNames  # TODO: Check what the implication of this is

        # TODO: Run Functions in Parallel

        # May need to move this outside of the function?
        # this should be ok
        @ray.remote
        def helperChangeDir(dir_path, f, literals):
            os.chdir(dir_path)
            f(literals)

        # perhaps ray.init() here? Also should ray.init(redirect_output=True) be used?
        ray.init()
        remaining_ids = []

        for i in range(numTrials):
            print(i)
            # FIXME: Add check if number of combinations of literals == numTrials??
            dir_path = tmpexperiment + '/' + str(i)
            # literals = list(map(lambda x: self.xp_state.literalNameToObj[x].v, lambdas[0][1]))
            f = lambdas[0][0]
            print(literals)
            remaining_ids.append(helperChangeDir.remote(dir_path, f, literals[i]))

        _, _ = ray.wait(remaining_ids, num_returns=numTrials)
        # _ = ray.get(remaining_ids) #I tried using get to see if there's a difference

        # Results directory initialization

        if not os.path.isdir(self.xp_state.versioningDirectory):
            os.mkdir(self.xp_state.versioningDirectory)

        moveBackFlag = False

        if os.path.exists(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0]):
            move(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0] + '/.git', '/tmp/')
            rmtree(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
            moveBackFlag = True

        # I took the liberty of adding the manifest code here. It looks like it will work without modification
        if manifest:
            os.chdir(tmpexperiment)

            dirs = [x for x in os.listdir() if util.isNumber(x)]
            table_full = []
            table_small = []

            for trial in dirs:
                os.chdir(trial)
                with open('.' + experimentName + '.jarvis', 'r') as fp:
                    config = json.load(fp)
                record_full = {}
                record_small = {}

                for literalName in literalNames:
                    # References the new config file ptr so is valid
                    record_full[literalName] = config[literalName]
                    record_small[literalName] = config[literalName]

                for artifactLabel in manifest:
                    record_full[artifactLabel] = util.loadArtifact(manifest[artifactLabel].loc)
                    if total_size(record_full[artifactLabel]) >= 1000:
                        record_small[artifactLabel] = " . . . "
                    else:
                        record_small[artifactLabel] = record_full[artifactLabel]
                    if util.isNumber(record_full[artifactLabel]):
                        record_full[artifactLabel] = eval(record_full[artifactLabel])
                    if util.isNumber(record_small[artifactLabel]):
                        record_small[artifactLabel] = eval(record_small[artifactLabel])
                record_small['__trialNum__'] = trial
                record_full['__trialNum__'] = trial

                table_full.append(record_full)
                table_small.append(record_small)
                os.chdir('../')

            df = pd.DataFrame(table_small)
            util.pickleTo(df, experimentName + '.pkl')

            os.chdir(original_dir)

        # Move Files from isolated environments to the ~/jarvis.d directory
        copytree(tmpexperiment, self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])

        os.chdir(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
        if moveBackFlag:
            move('/tmp/.git', self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
            repo = git.Repo(os.getcwd())
            repo.git.add(A=True)
            repo.index.commit('incremental commit')
        else:
            repo = git.Repo.init(os.getcwd())
            repo.git.add(A=True)
            repo.index.commit('initial commit')
        os.chdir(original_dir)

        if manifest:
            return pd.DataFrame(table_full)



    def pull(self):

        # temporary fix for backward compatibility
        # FIXME: self.xp_state.versioningDirectory = os.path.expanduser('~') + '/' + 'jarvis.d'
        self.xp_state.versioningDirectory = 'jarvis.d'
        
        util.activate(self)
        userDefFiles = set(os.listdir()) - self.xp_state.ghostFiles
        try:
            while True:
                self.__pull__()
                self.__commit__()
                subtreeMaxed = util.master_pop(self.xp_state.literals)
                if subtreeMaxed:
                    break
        except Exception as e:
            try:
                intermediateFiles = set(self.loclist) - userDefFiles
                for file in intermediateFiles:
                    if os.path.exists(file):
                        os.remove(file)
            except Exception as ee:
                print(ee)
            self.xp_state.literals = []
            self.xp_state.ghostFiles = set([])
            raise e

        #file management
        intermediateFiles = set(self.loclist) - userDefFiles
        for file in intermediateFiles:
            os.remove(file)
        commitables = []
        for file in (userDefFiles & (set(self.loclist) | set(self.scriptNames))):
            copyfile(file, self.xp_state.versioningDirectory + '/' + file)
            commitables.append(file)
        #committing everything to git
        os.chdir(self.xp_state.versioningDirectory)
        repo = git.Repo(os.getcwd())
        repo.index.add(commitables)
        repo.index.commit("incremental commit")
        tree = repo.tree()
        with open('.jarvis', 'w') as f:
            for obj in tree:
                commithash = util.runProc("git log " + obj.path).replace('\n', ' ').split()[1]
                if obj.path != '.jarvis':
                    f.write(obj.path + " " + commithash + "\n")
        repo.index.add(['.jarvis'])
        repo.index.commit('.jarvis commit')
        os.chdir('../')
        self.xp_state.literals = []
        self.xp_state.ghostFiles = set([])

    def peek(self, func = lambda x: x):
        trueVersioningDir = self.xp_state.versioningDirectory
        self.xp_state.versioningDirectory = '1fdf8583bfd663e98918dea393e273cc'
        try:
            self.pull()
            os.chdir(self.xp_state.versioningDirectory)
            listdir = [x for x in filter(util.isNumber, os.listdir())]
            _dir = str(len(listdir))
            if util.isPickle(self.loc):
                out = func(util.unpickle(_dir + '/' + self.loc))
            else:
                with open(_dir + '/' + self.loc, 'r') as f:
                    out = func(f.readlines())
            os.chdir('../')
        except Exception as e:
            out = e
        try:
            rmtree(self.xp_state.versioningDirectory)
        except:
            pass
        self.xp_state.versioningDirectory = trueVersioningDir
        return out

    def plot(self, rankdir=None):
        # WARNING: can't plot before pulling.
        # Prep globals, passed through arguments

        self.xp_state.nodes = {}
        self.xp_state.edges = []

        dot = Digraph()
        diagram = {"dot": dot, "counter": 0, "sha": {}}

        # with open('jarvis.d/.jarvis') as csvfile:
        #     reader = csv.reader(csvfile, delimiter=' ')
        #     for row in reader:
        #         ob, sha = row
        #         diagram["sha"][ob] = sha

        if not util.isOrphan(self):
            self.parent.__plotWalk__(diagram)
        else:
            node_diagram_id = str(diagram["counter"])
            dot.node(node_diagram_id, self.loc, shape="box")
            self.xp_state.nodes[self.loc] = node_diagram_id


        dot.format = 'png'
        if rankdir == 'LR':
            dot.attr(rankdir='LR')
        dot.render('driver.gv', view=True)
        # return self.xp_state.edges

    def getLocation(self):
        return self.loc
