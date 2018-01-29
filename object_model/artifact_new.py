def parallelPull(self, manifest={}):

	#TODO: Where to initialize Ray?

	self.xp_state.versioningDirectory = os.path.expanduser('~') + '/' + 'jarvis.d'

	tmpexperiment = self.xp_state.tmpexperiment
	if os.path.exists(tmpexperiment):
		rmtree(tmpexperiment)
	os.mkdir(tmpexperiment)

	self.xp_state.visited = []

	if not util.isOrphan(self):
		self.loclist = list(map(lambda x: x.getLocation(), self.parent.out_artifacts))
	else:
		self.loclist = [self.getLocation(),]
	self.scriptNames = []

	literalsAttached = set([])
	lambdas = []

	if not util.isOrphan(self):
		self.parent.__serialize__(lambdas, self.loclist, self.scriptNames)

	self.loclist = list(set(self.loclist))
	self.scriptNames = list(set(self.scriptNames)) #FIXME: Needed?

	self.loclist.sort()
	self.scriptNames.sort() #FIXME: Needed?

	for _, names in lambdas:
		literalsAttached |= set(names)

	original_dir = os.getcwd()
	experimentName = self.xp_state.jarvisFile.split('.')[0]

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
					literals.append((self.xp_state.literalNameToObj[kee].v, ))
				else:
					literals.append([self.xp_state.literalNameToObj[kee].v, ])
			literalNames.append(kee)
	
	literals = list(itertools.product(*literals))

	for i in range(numTrials):
		dst = tmpexperiment + '/' + str(i)
		copytree(os.getcwd(), dst, True) #TODO: Check if needed

	ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S') #TODO: Check if needed?
	self.xp_state.ray['literalNames'] = literalNames #TODO: Check what the implication of this is

	#TODO: Run Functions in Parallel 

	#May need to move this outside of the function?
	@ray.remote
	def helperChangeDir(dir_path, f, literals):
		os.chdir(dir_path)
		f(literals)

	remaining_ids = []
	for i in range(numTrials):
		#FIXME: Add check if number of combinations of literals == numTrials??
		dir_path = tmpexperiment + '/' + str(i)
		literals = list(map(lambda x: self.xp_state.literalNameToObj[x].v, lambdas[i][1]))
		f = lambdas[i][0]

		remaining_ids.append(helperChangeDir.remote(dir_path, f, literals))

	_, _ = ray.wait(remaining_ids, num_returns=numTrials)

	#Results directory initialization
	
	if not os.path.isdir(self.xp_state.versioningDirectory):
		os.mkdir(self.xp_state.versioningDirectory)

	moveBackFlag = False

	if os.path.exists(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0]):
		move(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0] + '/.git', '/tmp/')
		rmtree(self.xp_state.versioningDirectory + '/' + self.xp_state.jarvisFile.split('.')[0])
		moveBackFlag = True

	#TODO: Add in the manifest condition (L 275 - 313 and L 329-331)

	#Move Files from isolated environments to the ~/jarvis.d directory
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

