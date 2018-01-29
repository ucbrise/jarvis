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

	ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
	self.xp_state.ray['literalNames'] = literalNames

	#TODO: Run Functions in Parallel 

	#Tmp directory initialization

	#Modularize into helper Ray function

	# for f, names in lambdas:
	# 	literals = list(map(lambda x: self.xp_state.literalNameToObj[x].v, names))
	# 	f(literals)

	for _ in range(numTrials):
		


