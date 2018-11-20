/*
 *  Copyright 2016, 2017 IBM, DTCC, Fujitsu Australia Software Technology, IBM - All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *        http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.trivadis.hyperledger.backend;

import org.hyperledger.fabric.sdk.helper.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Config allows for a global config of the toolkit. Central location for all
 * toolkit configuration defaults. Has a local config file that can override any
 * property defaults. Config file can be relocated via a system property
 * "org.hyperledger.fabric.sdk.configuration". Any property can be overridden
 * with environment variable and then overridden
 * with a java system property. Property hierarchy goes System property
 * overrides environment variable which overrides config file for default values specified here.
 */

/**
 * Test Configuration
 */

public class TestConfig {
   private static final Logger log = LoggerFactory.getLogger(TestConfig.class);

    private static final String PROPBASE = "org.hyperledger.fabric.sdktest.";

    private static final String INVOKEWAITTIME = PROPBASE + "InvokeWaitTime";
    private static final String DEPLOYWAITTIME = PROPBASE + "DeployWaitTime";
    private static final String PROPOSALWAITTIME = PROPBASE + "ProposalWaitTime";

    private static final String INTEGRATIONTESTS_ORG = PROPBASE + "integrationTests.org.";
    private static final Pattern orgPat = Pattern.compile("^" + Pattern.quote(INTEGRATIONTESTS_ORG) + "([^\\.]+)\\.mspid$");

    private static final String INTEGRATIONTESTSTLS = PROPBASE + "integrationtests.tls";
    public final String FAB_CONFIG_GEN_VERS = "v1.3";

    private static TestConfig config;
    private static final Properties sdkProperties = new Properties();
    private final boolean runningTLS;
    private final boolean runningFabricCATLS;

    public boolean isRunningFabricTLS() {
        return runningFabricTLS;
    }

    private final boolean runningFabricTLS;
    private final HashMap<String, SampleOrg> sampleOrgs = new HashMap<>();

    private TestConfig() {

        try {
            sdkProperties.load(TestConfig.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {


            runningTLS = null != sdkProperties.getProperty(INTEGRATIONTESTSTLS, null);
            runningFabricCATLS = runningTLS;
            runningFabricTLS = runningTLS;


            for (Map.Entry<Object, Object> x : sdkProperties.entrySet()) {
                final String key = x.getKey() + "";
                final String val = x.getValue() + "";

                if (key.startsWith(INTEGRATIONTESTS_ORG)) {

                    Matcher match = orgPat.matcher(key);

                    if (match.matches() && match.groupCount() == 1) {
                        String orgName = match.group(1).trim();
                        sampleOrgs.put(orgName, new SampleOrg(orgName, val.trim()));

                    }
                }
            }

            for (Map.Entry<String, SampleOrg> org : sampleOrgs.entrySet()) {
                final SampleOrg sampleOrg = org.getValue();
                final String orgName = org.getKey();

                String peerNames = sdkProperties.getProperty(INTEGRATIONTESTS_ORG + orgName + ".peer_locations");
                String[] ps = peerNames.split("[ \t]*,[ \t]*");
                for (String peer : ps) {
                    String[] nl = peer.split("[ \t]*@[ \t]*");
                    sampleOrg.addPeerLocation(nl[0], grpcTLSify(nl[1]));
                }

                final String domainName = sdkProperties.getProperty(INTEGRATIONTESTS_ORG + orgName + ".domname");

                sampleOrg.setDomainName(domainName);

                String ordererNames = sdkProperties.getProperty(INTEGRATIONTESTS_ORG + orgName + ".orderer_locations");
                ps = ordererNames.split("[ \t]*,[ \t]*");
                for (String peer : ps) {
                    String[] nl = peer.split("[ \t]*@[ \t]*");
                    sampleOrg.addOrdererLocation(nl[0], grpcTLSify(nl[1]));
                }


                sampleOrg.setCALocation(httpTLSify(sdkProperties.getProperty((INTEGRATIONTESTS_ORG + org.getKey() + ".ca_location"))));

                sampleOrg.setCAName(sdkProperties.getProperty((INTEGRATIONTESTS_ORG + org.getKey() + ".caName")));

                if (runningFabricCATLS) {
                    String cert = "network/e2e-2Orgs/FAB_CONFIG_GEN_VERS/crypto-config/peerOrganizations/DNAME/ca/ca.DNAME-cert.pem"
                            .replaceAll("DNAME", domainName).replaceAll("FAB_CONFIG_GEN_VERS", FAB_CONFIG_GEN_VERS);
                    File cf = new File(cert);
                    if (!cf.exists() || !cf.isFile()) {
                        throw new RuntimeException("TEST is missing cert file " + cf.getAbsolutePath());
                    }
                    Properties properties = new Properties();
                    properties.setProperty("pemFile", cf.getAbsolutePath());

                    properties.setProperty("allowAllHostNames", "true"); //testing environment only NOT FOR PRODUCTION!

                    sampleOrg.setCAProperties(properties);
                }
            }

        }

    }

    public String getFabricConfigGenVers() {
        return FAB_CONFIG_GEN_VERS;
    }


    private String grpcTLSify(String location) {
        location = location.trim();
        Exception e = Utils.checkGrpcUrl(location);
        if (e != null) {
            throw new RuntimeException(String.format("Bad TEST parameters for grpc url %s", location), e);
        }
        return runningFabricTLS ?
                location.replaceFirst("^grpc://", "grpcs://") : location;

    }

    private String httpTLSify(String location) {
        location = location.trim();

        return runningFabricCATLS ?
                location.replaceFirst("^http://", "https://") : location;
    }

    /**
     * getConfig return back singleton for SDK configuration.
     *
     * @return Global configuration
     */
    public static TestConfig getConfig() {
        if (null == config) {
            config = new TestConfig();
        }
        return config;

    }

    /**
     * getProperty return back property for the given value.
     *
     * @param property
     * @return String value for the property
     */
    private String getProperty(String property) {

        String ret = sdkProperties.getProperty(property);

        if (null == ret) {
            log.warn(String.format("No configuration value found for '%s'", property));
        }
        return ret;
    }


    public int getTransactionWaitTime() {
        return Integer.parseInt(getProperty(INVOKEWAITTIME));
    }

    public int getDeployWaitTime() {
        return Integer.parseInt(getProperty(DEPLOYWAITTIME));
    }

    public long getProposalWaitTime() {
        return Integer.parseInt(getProperty(PROPOSALWAITTIME));
    }

    public Collection<SampleOrg> getIntegrationTestsSampleOrgs() {
        return Collections.unmodifiableCollection(sampleOrgs.values());
    }

    public SampleOrg getIntegrationTestsSampleOrg(String name) {
        return sampleOrgs.get(name);

    }

    public Properties getPeerProperties(String name,String networkPath) {

        return getEndPointProperties("peer", name,networkPath);

    }

    public Properties getOrdererProperties(String name,String networkPath) {

        return getEndPointProperties("orderer", name,networkPath);

    }

    public Properties getEndPointProperties(final String type, final String name,String networkPath) {
        Properties ret = new Properties();

        final String domainName = getDomainName(name);

        File cert = Paths.get(getTestChannelPath(networkPath), "crypto-config/ordererOrganizations".replace("orderer", type), domainName, type + "s",
                name, "tls/server.crt").toFile();
        if (!cert.exists()) {
            throw new RuntimeException(String.format("Missing cert file for: %s. Could not find at location: %s", name,
                    cert.getAbsolutePath()));
        }

        ret.setProperty("pemFile", cert.getAbsolutePath());

        ret.setProperty("hostnameOverride", name);
        ret.setProperty("sslProvider", "openSSL");
        ret.setProperty("negotiationType", "TLS");

        return ret;
    }

    public Properties getEventHubProperties(String name,String networkPath) {

        return getEndPointProperties("peer", name,networkPath); //uses same as named peer

    }

    public String getTestChannelPath(String networkPath) {

        return networkPath+"/e2e-2Orgs/" + FAB_CONFIG_GEN_VERS;

    }


    private String getDomainName(final String name) {
        int dot = name.indexOf(".");
        if (-1 == dot) {
            return null;
        } else {
            return name.substring(dot + 1);
        }

    }

}
