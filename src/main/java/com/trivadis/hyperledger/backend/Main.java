package com.trivadis.hyperledger.backend;

import org.bouncycastle.openssl.PEMWriter;
import org.hyperledger.fabric.sdk.*;
import org.hyperledger.fabric.sdk.helper.Config;
import org.hyperledger.fabric.sdk.security.CryptoSuite;
import org.hyperledger.fabric_ca.sdk.EnrollmentRequest;
import org.hyperledger.fabric_ca.sdk.HFCAClient;
import org.hyperledger.fabric_ca.sdk.HFCAInfo;
import org.hyperledger.fabric_ca.sdk.RegistrationRequest;

import java.io.File;
import java.io.StringWriter;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hyperledger.fabric.sdk.Channel.NOfEvents.createNofEvents;
import static org.hyperledger.fabric.sdk.Channel.PeerOptions.createPeerOptions;
import static org.hyperledger.fabric.sdk.Channel.TransactionOptions.createTransactionOptions;

public class Main {
  private static final TestConfig testConfig = TestConfig.getConfig();
  private static final String TEST_ADMIN_NAME = "admin";
  private static final String FOO_CHANNEL_NAME = "foo";
  private static final String testUser1 = "user1";
  private static final String NETWORK_PATH="network";
  private static final String CHAINCODE_PATH="chaincode";
  private static final String CHAIN_CODE_NAME = "example_cc_java";
  private static final String CHAIN_CODE_VERSION = "1";
  private static final String CHAIN_CODE_FILEPATH = "sample1";
  private static final TransactionRequest.Type CHAIN_CODE_LANG = TransactionRequest.Type.JAVA;
  private static final int DEPLOYWAITTIME = testConfig.getDeployWaitTime();

  private final TestConfigHelper configHelper = new TestConfigHelper();

  private Collection<SampleOrg> testSampleOrgs;
  private File sampleStoreFile = new File(System.getProperty("java.io.tmpdir") + "/HFCSampletest.properties");
  private SampleStore sampleStore = null;

  private Map<String, Properties> clientTLSProperties = new HashMap<>();


  private void createUsers() throws Exception {

    resetConfig();

    configHelper.customizeConfig();

    testSampleOrgs = testConfig.getIntegrationTestsSampleOrgs();
    //Set up hfca for each sample org

    for (SampleOrg sampleOrg : testSampleOrgs) {
      String caName = sampleOrg.getCAName(); //Try one of each name and no name.
      if (caName != null && !caName.isEmpty()) {
        sampleOrg.setCAClient(org.hyperledger.fabric_ca.sdk.HFCAClient.createNewInstance(caName, sampleOrg.getCALocation(), sampleOrg.getCAProperties()));
      } else {
        sampleOrg.setCAClient(HFCAClient.createNewInstance(sampleOrg.getCALocation(), sampleOrg.getCAProperties()));
      }
    }

    if (sampleStoreFile.exists()) { //For testing start fresh
      sampleStoreFile.delete();
    }
    sampleStore = new SampleStore(sampleStoreFile);
    enrollUsersSetup(sampleStore);

    HFClient client = HFClient.createNewInstance();

    client.setCryptoSuite(CryptoSuite.Factory.getCryptoSuite());

    SampleOrg sampleOrg = testConfig.getIntegrationTestsSampleOrg("peerOrg1");
    Channel channel = constructChannel(FOO_CHANNEL_NAME, client, sampleOrg);
    sampleStore.saveChannel(channel);
    install(client, channel, sampleOrg);
    instantiate(client, channel);
    move(client,channel,sampleOrg);
    query(client,channel);

  }


  private void query(HFClient client, Channel channel) throws Exception {
    final ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(CHAIN_CODE_NAME)
        .setVersion(CHAIN_CODE_VERSION).build();


    QueryByChaincodeRequest queryByChaincodeRequest = client.newQueryProposalRequest();
    queryByChaincodeRequest.setArgs(new String[] {"b"});
    queryByChaincodeRequest.setFcn("query");
    queryByChaincodeRequest.setChaincodeID(chaincodeID);

    Map<String, byte[]> tm2 = new HashMap<>();
    tm2.put("HyperLedgerFabric", "QueryByChaincodeRequest:JavaSDK".getBytes(UTF_8));
    tm2.put("method", "QueryByChaincodeRequest".getBytes(UTF_8));
    queryByChaincodeRequest.setTransientMap(tm2);

    Collection<ProposalResponse> queryProposals = channel.queryByChaincode(queryByChaincodeRequest, channel.getPeers());
    for (ProposalResponse proposalResponse : queryProposals) {
      if (!proposalResponse.isVerified() || proposalResponse.getStatus() != ProposalResponse.Status.SUCCESS) {
      } else {
        String payload = proposalResponse.getProposalResponse().getResponse().getPayload().toStringUtf8();

        System.out.println("---------------------> "+payload);

      }
    }


  }

  private void move(HFClient client, Channel channel, SampleOrg sampleOrg)  throws Exception {

    final ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(CHAIN_CODE_NAME)
        .setVersion(CHAIN_CODE_VERSION).build();
    Collection<ProposalResponse> responses;
    Collection<ProposalResponse> successful = new LinkedList<>();
    Collection<ProposalResponse> failed = new LinkedList<>();


    client.setUserContext(sampleOrg.getUser(testUser1));

    ///////////////
    /// Send transaction proposal to all peers
    TransactionProposalRequest transactionProposalRequest = client.newTransactionProposalRequest();
    transactionProposalRequest.setChaincodeID(chaincodeID);
    transactionProposalRequest.setChaincodeLanguage(CHAIN_CODE_LANG);
    transactionProposalRequest.setFcn("invoke");
    //transactionProposalRequest.setFcn("move");
    transactionProposalRequest.setProposalWaitTime(testConfig.getProposalWaitTime());
    transactionProposalRequest.setArgs("a", "b", "100");

    Map<String, byte[]> tm2 = new HashMap<>();
    tm2.put("HyperLedgerFabric", "TransactionProposalRequest:JavaSDK".getBytes(UTF_8)); //Just some extra junk in transient map
    tm2.put("method", "TransactionProposalRequest".getBytes(UTF_8)); // ditto
    tm2.put("result", ":)".getBytes(UTF_8));  // This should be returned in the payload see chaincode why.

    transactionProposalRequest.setTransientMap(tm2);


    //  Collection<ProposalResponse> transactionPropResp = channel.sendTransactionProposalToEndorsers(transactionProposalRequest);
    Collection<ProposalResponse> transactionPropResp = channel.sendTransactionProposal(transactionProposalRequest, channel.getPeers());
    for (ProposalResponse response : transactionPropResp) {
      if (response.getStatus() == ProposalResponse.Status.SUCCESS) {
        successful.add(response);
      } else {
        failed.add(response);
      }
    }

    if (failed.size() > 0) {
      ProposalResponse firstTransactionProposalResponse = failed.iterator().next();
      //fail
    }

    // Check that all the proposals are consistent with each other. We should have only one set
    // where all the proposals above are consistent. Note the when sending to Orderer this is done automatically.
    //  Shown here as an example that applications can invoke and select.
    // See org.hyperledger.fabric.sdk.proposal.consistency_validation config property.
    Collection<Set<ProposalResponse>> proposalConsistencySets = SDKUtils.getProposalConsistencySets(transactionPropResp);
    if (proposalConsistencySets.size() != 1) {
      //fail
    }

    //  System.exit(10);

    ProposalResponse resp = successful.iterator().next();
    byte[] x = resp.getChaincodeActionResponsePayload(); // This is the data returned by the chaincode.
    String resultAsString = null;
    if (x != null) {
      resultAsString = new String(x, UTF_8);
    }
    System.out.println(resultAsString);


    TxReadWriteSetInfo readWriteSetInfo = resp.getChaincodeActionResponseReadWriteSetInfo();
    //See blockwalker below how to transverse this

    ChaincodeID cid = resp.getChaincodeID();
    final String path = cid.getPath();


    ////////////////////////////
    // Send Transaction Transaction to orderer
    BlockEvent.TransactionEvent transactionEvent = channel.sendTransaction(successful).get(testConfig.getTransactionWaitTime(), TimeUnit.SECONDS);

    System.out.println(transactionEvent);

  }

  private void instantiate(HFClient client, Channel channel)throws Exception {

    final ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(CHAIN_CODE_NAME)
        .setVersion(CHAIN_CODE_VERSION).build();
    Collection<ProposalResponse> responses;
    Collection<ProposalResponse> successful = new LinkedList<>();
    Collection<ProposalResponse> failed = new LinkedList<>();

    ///////////////
    //// Instantiate chaincode.
    InstantiateProposalRequest instantiateProposalRequest = client.newInstantiationProposalRequest();
    instantiateProposalRequest.setProposalWaitTime(DEPLOYWAITTIME);
    instantiateProposalRequest.setChaincodeID(chaincodeID);
    instantiateProposalRequest.setChaincodeLanguage(CHAIN_CODE_LANG);
    instantiateProposalRequest.setFcn("init");
    instantiateProposalRequest.setArgs(new String[]{"a", "500", "b", "200" });
    Map<String, byte[]> tm = new HashMap<>();
    tm.put("HyperLedgerFabric", "InstantiateProposalRequest:JavaSDK".getBytes(UTF_8));
    tm.put("method", "InstantiateProposalRequest".getBytes(UTF_8));
    instantiateProposalRequest.setTransientMap(tm);

    ChaincodeEndorsementPolicy chaincodeEndorsementPolicy = new ChaincodeEndorsementPolicy();
    chaincodeEndorsementPolicy.fromYamlFile(new File(NETWORK_PATH + "/chaincodeendorsementpolicy.yaml"));
    instantiateProposalRequest.setChaincodeEndorsementPolicy(chaincodeEndorsementPolicy);

    successful.clear();
    failed.clear();

      responses = channel.sendInstantiationProposal(instantiateProposalRequest, channel.getPeers());
    for (ProposalResponse response : responses) {
      if (response.isVerified() && response.getStatus() == ProposalResponse.Status.SUCCESS) {
        successful.add(response);
      } else {
        failed.add(response);
      }
    }
    if (failed.size() > 0) {
      for (ProposalResponse fail : failed) {


      }
      ProposalResponse first = failed.iterator().next();
    }
    ///////////////
    /// Send instantiate transaction to orderer

    //Specify what events should complete the interest in this transaction. This is the default
    // for all to complete. It's possible to specify many different combinations like
    //any from a group, all from one group and just one from another or even None(NOfEvents.createNoEvents).
    // See. Channel.NOfEvents
    Channel.NOfEvents nOfEvents = createNofEvents();
    if (!channel.getPeers(EnumSet.of(Peer.PeerRole.EVENT_SOURCE)).isEmpty()) {
      nOfEvents.addPeers(channel.getPeers(EnumSet.of(Peer.PeerRole.EVENT_SOURCE)));
    }
    if (!channel.getEventHubs().isEmpty()) {
      nOfEvents.addEventHubs(channel.getEventHubs());
    }

    BlockEvent.TransactionEvent transactionEvent = channel.sendTransaction(successful, createTransactionOptions() //Basically the default options but shows it's usage.
        .userContext(client.getUserContext()) //could be a different user context. this is the default.
        .shuffleOrders(false) // don't shuffle any orderers the default is true.
        .orderers(channel.getOrderers()) // specify the orderers we want to try this transaction. Fails once all Orderers are tried.
        .nOfEvents(nOfEvents) // The events to signal the completion of the interest in the transaction
    ).get();

    System.out.println(transactionEvent);
  }

  private void install(HFClient client, Channel channel, SampleOrg sampleOrg) throws Exception {

    final ChaincodeID chaincodeID = ChaincodeID.newBuilder().setName(CHAIN_CODE_NAME)
        .setVersion(CHAIN_CODE_VERSION).build();
    Collection<ProposalResponse> responses;
    Collection<ProposalResponse> successful = new LinkedList<>();
    Collection<ProposalResponse> failed = new LinkedList<>();

    client.setUserContext(sampleOrg.getPeerAdmin());


    InstallProposalRequest installProposalRequest = client.newInstallProposalRequest();
    installProposalRequest.setChaincodeID(chaincodeID);

    // on foo chain install from directory.

    ////For GO language and serving just a single user, chaincodeSource is mostly likely the users GOPATH
    installProposalRequest.setChaincodeSourceLocation(Paths.get(CHAINCODE_PATH, CHAIN_CODE_FILEPATH).toFile());

    if (testConfig.isFabricVersionAtOrAfter("1.1")) { // Fabric 1.1 added support for  META-INF in the chaincode image.

      //This sets an index on the variable a in the chaincode // see http://hyperledger-fabric.readthedocs.io/en/master/couchdb_as_state_database.html#using-couchdb-from-chaincode
      // The file IndexA.json as part of the META-INF will be packaged with the source to create the index.
      installProposalRequest.setChaincodeMetaInfLocation(new File(NETWORK_PATH));
    }

    installProposalRequest.setChaincodeVersion(CHAIN_CODE_VERSION);
    installProposalRequest.setChaincodeLanguage(CHAIN_CODE_LANG);

    ////////////////////////////
    // only a client from the same org as the peer can issue an install request
    int numInstallProposal = 0;

    Collection<Peer> peers = channel.getPeers();
    numInstallProposal = numInstallProposal + peers.size();
    responses = client.sendInstallProposal(installProposalRequest, peers);

    for (ProposalResponse response : responses) {
      if (response.getStatus() == ProposalResponse.Status.SUCCESS) {
        successful.add(response);
      } else {
        failed.add(response);
      }
    }


  }

  Channel constructChannel(String name, HFClient client, SampleOrg sampleOrg) throws Exception {

    //Only peer Admin org
    SampleUser peerAdmin = sampleOrg.getPeerAdmin();
    client.setUserContext(peerAdmin);

    Collection<Orderer> orderers = new LinkedList<>();

    for (String orderName : sampleOrg.getOrdererNames()) {

      Properties ordererProperties = testConfig.getOrdererProperties(orderName);

      //example of setting keepAlive to avoid timeouts on inactive http2 connections.
      // Under 5 minutes would require changes to server side to accept faster ping rates.
      ordererProperties.put("grpc.NettyChannelBuilderOption.keepAliveTime", new Object[]{5L, TimeUnit.MINUTES});
      ordererProperties.put("grpc.NettyChannelBuilderOption.keepAliveTimeout", new Object[]{8L, TimeUnit.SECONDS});
      ordererProperties.put("grpc.NettyChannelBuilderOption.keepAliveWithoutCalls", new Object[]{true});

      orderers.add(client.newOrderer(orderName, sampleOrg.getOrdererLocation(orderName),
          ordererProperties));
    }

    //Just pick the first orderer in the list to create the channel.

    Orderer anOrderer = orderers.iterator().next();
    orderers.remove(anOrderer);

    String path = NETWORK_PATH + "/e2e-2Orgs/" + testConfig.getFabricConfigGenVers() + "/" + name + ".tx";
    ChannelConfiguration channelConfiguration = new ChannelConfiguration(new File(path));

    //Create channel that has only one signer that is this orgs peer admin. If channel creation policy needed more signature they would need to be added too.
    Channel newChannel = client.newChannel(name, anOrderer, channelConfiguration, client.getChannelConfigurationSignature(channelConfiguration, peerAdmin));


    boolean everyother = true; //test with both cases when doing peer eventing.
    for (String peerName : sampleOrg.getPeerNames()) {
      String peerLocation = sampleOrg.getPeerLocation(peerName);

      Properties peerProperties = testConfig.getPeerProperties(peerName); //test properties for peer.. if any.
      if (peerProperties == null) {
        peerProperties = new Properties();
      }

      //Example of setting specific options on grpc's NettyChannelBuilder
      peerProperties.put("grpc.NettyChannelBuilderOption.maxInboundMessageSize", 9000000);

      Peer peer = client.newPeer(peerName, peerLocation, peerProperties);
      if (testConfig.isFabricVersionAtOrAfter("1.3")) {
        newChannel.joinPeer(peer, createPeerOptions().setPeerRoles(EnumSet.of(Peer.PeerRole.ENDORSING_PEER, Peer.PeerRole.LEDGER_QUERY, Peer.PeerRole.CHAINCODE_QUERY, Peer.PeerRole.EVENT_SOURCE))); //Default is all roles.

      } else {
        // Set peer to not be all roles but eventing.
        newChannel.joinPeer(peer, createPeerOptions().setPeerRoles(EnumSet.of(Peer.PeerRole.ENDORSING_PEER, Peer.PeerRole.LEDGER_QUERY, Peer.PeerRole.CHAINCODE_QUERY)));
      }
      everyother = !everyother;
    }

    for (Orderer orderer : orderers) { //add remaining orderers if any.
      newChannel.addOrderer(orderer);
    }

    for (String eventHubName : sampleOrg.getEventHubNames()) {

      final Properties eventHubProperties = testConfig.getEventHubProperties(eventHubName);

      eventHubProperties.put("grpc.NettyChannelBuilderOption.keepAliveTime", new Object[]{5L, TimeUnit.MINUTES});
      eventHubProperties.put("grpc.NettyChannelBuilderOption.keepAliveTimeout", new Object[]{8L, TimeUnit.SECONDS});

      EventHub eventHub = client.newEventHub(eventHubName, sampleOrg.getEventHubLocation(eventHubName),
          eventHubProperties);
      newChannel.addEventHub(eventHub);
    }

    newChannel.initialize();


    //Just checks if channel can be serialized and deserialized .. otherwise this is just a waste :)
    byte[] serializedChannelBytes = newChannel.serializeChannel();
    newChannel.shutdown(true);

    return client.deSerializeChannel(serializedChannelBytes).initialize();

  }


  private void enrollUsersSetup(SampleStore a) throws Exception {

    for (SampleOrg sampleOrg : testSampleOrgs) {

      HFCAClient ca = sampleOrg.getCAClient();

      final String orgName = sampleOrg.getName();
      final String mspid = sampleOrg.getMSPID();
      ca.setCryptoSuite(org.hyperledger.fabric.sdk.security.CryptoSuite.Factory.getCryptoSuite());

      if (testConfig.isRunningFabricTLS()) {
        //This shows how to get a client TLS certificate from Fabric CA
        // we will use one client TLS certificate for orderer peers etc.
        final EnrollmentRequest enrollmentRequestTLS = new EnrollmentRequest();
        enrollmentRequestTLS.addHost("localhost");
        enrollmentRequestTLS.setProfile("tls");
        final Enrollment enroll = ca.enroll("admin", "adminpw", enrollmentRequestTLS);
        final String tlsCertPEM = enroll.getCert();
        final String tlsKeyPEM = getPEMStringFromPrivateKey(enroll.getKey());

        final Properties tlsProperties = new Properties();

        tlsProperties.put("clientKeyBytes", tlsKeyPEM.getBytes(UTF_8));
        tlsProperties.put("clientCertBytes", tlsCertPEM.getBytes(UTF_8));
        clientTLSProperties.put(sampleOrg.getName(), tlsProperties);
        //Save in samplestore for follow on tests.
        sampleStore.storeClientPEMTLCertificate(sampleOrg, tlsCertPEM);
        sampleStore.storeClientPEMTLSKey(sampleOrg, tlsKeyPEM);
      }

      HFCAInfo info = ca.info(); //just check if we connect at all.
      String infoName = info.getCAName();

      SampleUser admin = sampleStore.getMember(TEST_ADMIN_NAME, orgName);
      if (!admin.isEnrolled()) {  //Preregistered admin only needs to be enrolled with Fabric caClient.
        admin.setEnrollment(ca.enroll(admin.getName(), "adminpw"));
        admin.setMspId(mspid);
      }

      SampleUser user = sampleStore.getMember(testUser1, sampleOrg.getName());
      if (!user.isRegistered()) {  // users need to be registered AND enrolled
        RegistrationRequest rr = new RegistrationRequest(user.getName(), "org1.department1");
        user.setEnrollmentSecret(ca.register(rr, admin));
      }
      if (!user.isEnrolled()) {
        user.setEnrollment(ca.enroll(user.getName(), user.getEnrollmentSecret()));
        user.setMspId(mspid);
      }

      final String sampleOrgName = sampleOrg.getName();
      final String sampleOrgDomainName = sampleOrg.getDomainName();

      SampleUser peerOrgAdmin = sampleStore.getMember(sampleOrgName + "Admin", sampleOrgName, sampleOrg.getMSPID(),
          Util.findFileSk(Paths.get(testConfig.getTestChannelPath(), "crypto-config/peerOrganizations/",
              sampleOrgDomainName, format("/users/Admin@%s/msp/keystore", sampleOrgDomainName)).toFile()),
          Paths.get(testConfig.getTestChannelPath(), "crypto-config/peerOrganizations/", sampleOrgDomainName,
              format("/users/Admin@%s/msp/signcerts/Admin@%s-cert.pem", sampleOrgDomainName, sampleOrgDomainName)).toFile());
      sampleOrg.setPeerAdmin(peerOrgAdmin); //A special user that can create channels, join peers and install chaincode

      sampleOrg.addUser(user);
      sampleOrg.setAdmin(admin); // The admin of this org --
    }
  }


  public static void main(String[] args) throws Exception {
    Main main = new Main();
    main.createUsers();
  }


  private String getPEMStringFromPrivateKey(java.security.PrivateKey privateKey) throws Exception {
    StringWriter pemStrWriter = new StringWriter();
    PEMWriter pemWriter = new PEMWriter(pemStrWriter);

    pemWriter.writeObject(privateKey);

    pemWriter.close();

    return pemStrWriter.toString();
  }

  private static String format(String format, Object... args) {
    return new java.util.Formatter().format(format, args).toString();
  }


  private static void resetConfig() {

    try {
      final java.lang.reflect.Field field = Config.class.getDeclaredField("config");
      field.setAccessible(true);
      field.set(Config.class, null);
      Config.getConfig();
    } catch (Exception e) {
      throw new RuntimeException("Cannot reset config", e);
    }

  }
}
