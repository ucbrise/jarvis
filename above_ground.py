
def newEperimentVersion(xp_state):
    # -- caution with fixed values like 'jarvisExperiment', allowing for early Ground Ref prototype

    # The name of this experiment is in a tag in the nodeVersion of 'jarvisExperiment'
    latest_experiment_node_versions = [x for x in xp_state.gc.getNodeLatestVersions('jarvisExperiment')
                                       if xp_state.gc.getNodeVersion(x).get_tags()['experimentName'][
                                           'value'] == xp_state.EXPERIMENT_NAME]

    # This experiment may have previous versions, then the most recents are the parents
    return xp_state.gc.createNodeVersion(xp_state.gc.getNode('jarvisExperiment').get_id(),
                                       tags={
                                           'experimentName': {
                                               'key': 'experimentName',
                                               'value': xp_state.EXPERIMENT_NAME,
                                               'type': 'string'
                                           }},
                                       parentIds=latest_experiment_node_versions)

def newTrialVersion(xp_state):
    return xp_state.gc.createNodeVersion(xp_state.gc.getNode('jarvisTrial').get_id())

def newLiteralVersion(xp_state, literalName, literalValue):
    return xp_state.gc.createNodeVersion(xp_state.gc.getNode('jarvisLiteral').get_id(),
                                         tags = {
                                             'literalName' : {
                                                 'key' : 'literalName',
                                                 'value' : literalName,
                                                 'type' : 'string'
                                             },
                                             'literalValue' : {
                                                 'key': 'literalValue',
                                                 'value' : str(literalValue),
                                                 'type' : 'string'
                                             }
                                         })

def newArtifactVersion(xp_state, artifactName):
    # Connect artifact versions to parents offline
    # What's important at this level is the tags
    # What artifact meta-data do we care about
    return xp_state.gc.createNodeVersion(xp_state.gc.getNode('jarvisArtifact').get_id(),
                                       tags={
                                           'artifactName': {
                                               'key': 'artifactName',
                                               'value': artifactName,
                                               'type': 'string'
                                           }
                                       })

def newExperimentTrialEdgeVersion(xp_state, fromNv, toNv):
    return __newEdgeVersion__(xp_state, fromNv, toNv, 'jarvisExperimentjarvisTrial')

def newTrialLiteralEdgeVersion(xp_state, fromNv, toNv):
    return __newEdgeVersion__(xp_state, fromNv, toNv, 'jarvisTrialjarvisLiteral')

def newTrialArtifactEdgeVersion(xp_state, fromNv, toNv):
    return __newEdgeVersion__(xp_state, fromNv, toNv, 'jarvisTrialjarvisArtifact')

def __newEdgeVersion__(xp_state, fromNv, toNv, edgeKey):
    return xp_state.gc.createEdgeVersion(xp_state.gc.getEdge(edgeKey).get_id(),
                                         fromNv.get_id(),
                                         toNv.get_id())