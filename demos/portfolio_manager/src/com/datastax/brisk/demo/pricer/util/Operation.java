/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.brisk.demo.pricer.util;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.datastax.brisk.demo.pricer.Session;
import com.datastax.brisk.demo.pricer.Pricer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public abstract class Operation
{
    public static String[] tickers = {"AEA","ASP","AIT","ATV","ACM","ACH","AVA","AEG","ASF","ARO","AMG","ASA","AHD","ALZ","AA","ATU","ARG","AEB","ASG","AEO","ADY","APL","AKT","AMP","AXE","AFG","AGD","AFB","AVT","AGM","APD","ATT","ADI","ASI","AWH","ABB","ARK","ALV","AVK","ARM","ATE","AHS","ARD","ABK","ABM","AMX","AKP","APF","AHT","AOL","AVB","APH","AM","AYN","ADM","ANH","AP","AZN","ATR","AKF","ALE","AAV","AU","AB","ANW","AEL","AWK","AMR","APC","ALL","ACC","ALQ","ACO","AVD","ACI","ALK","AWF","AHL","ACE","ANN","AFE","ART","ABX","ACF","APB","ATO","AIN","AVP","AIZ","AAP","AHC","ADX","ALD","ANF","AIB","ARE","AOS","AIG","AOD","AEE","ABG","ABV","AFF","AAI","ANR","ASH","AAN","AXP","AEM","AN","ARJ","AIQ","AFN","AKS","ALU","ABA","APX","ACG","AGC","AXR","ATI","AOI","ARB","ALJ","AGP","ASX","AXS","AOB","ALX","AES","ADS","ALEX","AMN","ALC","AEH","ALG","ACN","AFL","ADC","AGCO","AZO","AAR","ARL","AEC","AON","ALB","AMD","ABD","APA","AIV","AGU","AKR","AGO","ARI","AVY","ATW","ALF","ADP","ACL","AUY","AME","ATK","AMT","AWR","AER","AET","AIR","AYR","AVF","AJG","ASR","AMB","APU","AEP","AYE","AWI","AEF","ABVT","AWC","AUO","AEV","AFC","ACS","AED","AXA","ABT","AF","AV","ABR","AGN","AVX","AI","ALY","AXL","ARP","AZZ","ARW","AGL","AWP","ACV","ALM","ABC","AYI","BGY","BPL","BDJ","BXC","BRE","BKI","BX","BRO","BXP","BME","BPP","BFK","BTE","BKN","BXS","BG","BT","BYD","BIG","BBVA","BBF","BEC","BSC","BGR","BAF","BGG","BDN","BK","BWA","BDT","BGC","BRFS","BHD","BTA","BBX","BHP","BBV","BMO","BPZ","BC","BFS","BMT","BHL","BGS","BOH","BNJ","BID","BA","BWS","BCR","BSBR","BCS","BGP","BWP","BZH","BSP","BLW","BRY","BIE","BPI","BMC","BAM","BBW","BRF","BHE","BCE","BPK","BEP","BCF","BMR","BBY","BEN","BAK","BAP","BDF","BKS","BPO","BR","BP","BCK","BTZ","BSD","BIF","BKD","BXG","BEO","BNE","BNI","BVF","BKK","BKC","BKE","BCH","BSX","BMA","BZ","BBK","BGT","BLL","BTU","BBD","BCO","BSE","BJS","BPT","BFO","BFR","BSI","BBG","BE","BLX","BNY","BTO","BHK","BSY","BZA","BTF","BHY","BAC","BAS","BMS","BFZ","BTH","BGH","BDC","BMI","BZMD","BDK","BRC","BIO","BOE","BW","BQH","BLT","BVN","BRT","BNS","BCA","BRS","BIP","BNA","BAX","BIN","BHI","BLC","BLH","BEE","BJZ","BEZ","BBI","BDX","BYI","BWF","BDV","BKH","BMY","BQR","BLK","BBL","BYM","BKT","BWY","BJ","BBT","BUD","BHS","BTM","BLU","CLI","CSL","CMP","CVH","CUK","CAT","CSV","CQB","CME","CTB","CHT","CAP","CMS","CUZ","CYT","CLU","CHU","CRP","CVS","CJS","CVX","CSP","CY","COG","CSS","CBY","CHSP","CLW","CMM","CSR","CMN","CAJ","CJB","CRE","CA","CAH","CPL","CYD","CBE","CSA","CCS","CEC","CII","CBM","CBL","CPX","CHI","CXS","CVE","CI","CLD","CNX","CHP","CML","CW","CNK","CPF","CIF","CGI","CTC","CRA","CMC","CPE","CHH","CLC","CCT","CIR","CAE","CCC","CFT","CHN","CNS","CGO","CFR","CKP","CGX","CBG","CBT","CNH","CYN","CIG","CCI","CBR","CTS","CMU","CHS","CGA","CLF","CFN","CL","CCU","CNA","CRK","CBI","CAF","CHW","CAB","CCH","CHG","CTL","CVB","CRH","CCZ","CXH","CXE","CLR","CSJ","CLNY","CHA","CBS","CNI","CTV","CSU","CM","CSE","CJR","CRN","CSC","CP","CDE","CIX","CYH","CSQ","CIB","CPT","CRY","CPO","CBU","CCK","CZZ","CMO","CBK","CSK","CLP","CHC","CR","CIE","CWF","CRR","CLB","COY","COL","CFX","CBB","CIU","CPY","CDR","CE","CWT","CLX","CX","CPP","CVC","CMA","CIT","CPV","CHY","CGV","CS","CJA","CRL","CCO","CV","CBD","CEA","CO","COH","CVO","CXO","CSX","CVG","CNC","CJT","CBC","CWZ","CVI","CMK","CSH","COF","COT","CMG","CVD","CEG","CASC","CNN","CNL","CNQ","CMI","COP","CRM","CKR","CYS","CPN","CHL","CLH","CHK","COO","CACI","CATO","CB","CAG","CPC","CNW","CCW","CLS","CPB","CCL","CIA","CYE","CHE","CUB","CCJ","CEE","CFL","CCM","CCE","CKH","CT","CXW","CRI","CBZ","CN","CF","CEO","COV","CIM","CAS","CPK","CFI","CRS","CPA","CEL","CHD","CAM","CMZ","CAL","CDI","CXG","CNP","CNO","CHB","CRT","CVA","DEW","DKS","DSW","DGI","DUC","DRU","DCS","DOW","DV","DEO","DKW","DRL","DUA","DHT","DHR","DFT","DLN","DCT","DVN","DTK","DK","DTE","DTT","DKT","DAI","DFP","DG","DKQ","DFS","DPZ","DCW","DPD","DWA","DBD","DEXO","DHF","DHG","DEP","DIS","DYN","DF","DLM","DGX","DLR","DKC","DTF","DVD","DTN","DUF","DLX","DE","DHI","DRH","DTG","DNB","DAN","DPM","DEL","DFY","DCE","DJP","DKR","DTV","DIN","DVY","DCM","DKY","DRE","DKL","DAL","DDS","DCP","DES","DGF","DUK","DHX","DM","DKM","DXB","DDE","DKI","DKP","DEB","DVF","DVA","DEX","DBN","DVM","DSX","DGW","DOX","DKK","DPC","DCO","DLB","DNP","DPO","DKX","DO","DB","DAC","DRP","DVR","DOLE","DST","DD","DNY","DL","DRI","DT","DW","DCA","DRQ","DEG","DCI","DDF","DX","DEI","DRC","DSF","DON","DOV","DHM","DSM","DDT","DNR","DPS","DYP","DDR","DY","DKF","DAR","DSU","DFR","DPL","DOM","DFG","EXI","EWT","EOD","ELX","EMN","EWM","ENH","ERF","ESV","EPD","EAC","EWI","EL","EDS","EOT","ENR","EAT","EWJ","EWN","ETG","ETP","ETJ","ELN","ELS","EMD","EMO","EPP","EQY","EGY","ERJ","ETO","EVR","EPE","ENI","EFR","EGO","ENL","EEQ","EXG","EGP","ESI","ENB","EWY","EPR","EXM","EEM","EMS","EQT","EP","EEH","EDT","EMF","EWZ","EBR","ESC","EDE","EOG","EXC","ERO","EGF","EOC","ENS","EIG","EOI","EDN","ETW","EWK","EOE","EHL","EVC","EEP","EWQ","ESL","EK","EVN","EBS","ED","EJ","EMQ","ECA","EXH","EME","EWH","EVT","EIX","EWS","EE","ESD","ETH","EDR","ETN","EEA","ENT","EW","EPB","EQR","EDU","EV","EHB","ETM","EBF","EWD","EC","EPL","EMR","ENZ","EFG","EGN","EXR","ETV","ESS","EWP","EWU","ETB","EDD","EFT","EHA","EWL","EFV","EVG","EBI","ESE","ETR","ELP","EMC","ECL","EFA","EQS","EZA","ETE","ELY","EXP","ENP","EHI","EVF","EFX","ETY","ES","EM","EOS","FOE","FGB","FXI","FHO","FR","FJA","FCH","FFG","FMD","FMS","FPT","FE","FCN","FNF","FDI","FCT","FRB","FWF","FLR","FFH","FUR","FNB","FEU","FHN","FAV","FCF","FIF","FLC","FTI","FCS","FRZ","FTO","FGF","FRT","FTK","FMX","FEZ","FTR","FIX","FEO","FSR","FDG","FDO","FST","FCZ","FUN","FF","FT","FRP","FRE","FOF","FFC","FC","FTT","FSS","FFA","FLY","FIG","FHI","FPO","FMP","FSC","FO","FCX","FUL","FMR","FBR","FGC","FAM","FRX","FTE","FHY","FOR","FBP","FLS","FII","FPL","FMO","FMN","FGI","FGE","FL","FDX","FFD","FRA","FNA","FMY","FICO","FRM","FAC","FRO","FMC","FLO","FGP","FDS","FIS","FNM","FBN","FBC","FAF","FCY","FDP","FCJ","GJW","GFA","GPC","GRE","GB","GJN","GEO","GES","GG","GTI","GWR","GXP","GPI","GTY","GFY","GPD","GRB","GU","GEA","GJI","GMK","GLT","GS","GAP","GA","GTC","GVI","GY","GF","GKM","GDO","GJT","GIS","GLW","GNA","GGC","GMR","GEX","GJP","GGB","GCA","GSG","GCH","GUQ","GEF","GOL","GBX","GWW","GMT","GGT","GSC","GJS","GPX","GAI","GJM","GCI","GPW","GTN","GAM","GFZ","GBE","GRT","GAH","GPK","GDI","GHL","GSK","GGG","GDP","GNW","GCS","GEC","GPU","GFF","GBL","GEJ","GAB","GME","GPS","GAS","GE","GAJ","GSI","GPN","GJK","GUL","GLS","GER","GBB","GEG","GJR","GLF","GAT","GYC","GKK","GUT","GRA","GAR","GJJ","GJL","GOM","GYB","GIL","GNK","GSP","GDV","GJG","GHI","GYA","GJB","GJF","GFI","GNV","GJV","GJO","GLG","GUI","GRX","GFW","GLP","GCO","GEP","GPJ","GRS","GJE","GED","GRO","GJD","GR","GNI","GET","GJH","GCF","GSL","GOF","GSH","GIB","GIM","GD","GOV","GT","GJX","GDF","GMW","GDL","GTS","GMXR","GWF","GLD","GMA","GCV","GRR","GBF","GVA","HQL","HYT","HJA","HJE","HEW","HF","HIT","HCF","HJO","HCP","HPS","HTN","HJR","HW","HYY","HRB","HYA","HMC","HES","HEI","HYL","HE","HRG","HPI","HMH","HTE","HHY","HNZ","HWD","HOC","HJL","HMY","HPY","HJN","HPQ","HI","HGG","HCH","HAE","HIX","HSC","HON","HTZ","HL","HSA","HMA","HYK","HYV","HEP","HNT","HIO","HNI","HBC","HCC","HME","HRPN","HHS","HAR","HS","HZO","HYH","HTH","HTY","H","HLS","HR","HSM","HOV","HBI","HIG","HCN","HJG","HJJ","HOT","HLF","HNP","HST","HVT","HYM","HIH","HRC","HGT","HDB","HP","HPT","HTR","HJV","HK","HXM","HSP","HJT","HYB","HAL","HRZ","HTB","HAS","HPF","HOO","HUN","HSF","HMN","HT","HYF","HCS","HAV","HSY","HRP","HRS","HQH","HTS","HLX","HNR","HD","HIW","HIL","HOS","HRL","HEK","HXL","HYC","HOG","HIS","HTX","HZD","HTD","HIF","HGR","HCE","HUM","HZK","IDC","IXG","IEP","IKR","ITT","IDU","IN","IGT","IHF","IGI","IDA","IAI","IEX","IBI","IGM","IJH","IJT","IVW","ITB","IAK","IAT","IND","IYZ","ITA","IYR","IBA","IHC","IGA","IPG","IKM","ICA","IEV","IO","ISI","IHR","IFN","IYC","IKL","IXN","IOC","IDE","IWC","ICO","IBN","IRL","IT","ITUB","IEO","IYW","ILF","IRC","INZ","ISM","IYG","ISG","IWA","IYE","ITC","ITW","IMF","IIF","IMN","IQI","IID","IJK","IJJ","IBM","IVN","IYT","IVE","IHE","IM","IMT","IXP","IQN","IGV","ICB","IRS","IVC","IXJ","IVR","IRR","IIT","IDT","ITF","IHI","IXC","IYJ","IEI","IYH","INP","IVV","IJD","IGW","IHS","IEZ","IOO","ISP","IGN","IQM","IDG","ISH","IKJ","IJS","INB","IGE","IGK","IQT","IMA","IJR","IMS","ICS","ISF","IRM","IR","IAE","IFF","IQC","IMB","IGD","ITG","IRF","IRE","ING","INT","IMC","IP","ID","IYY","IIM","IGR","IYF","IAG","IIC","IVZ","IYM","IYK","ICE","IPI","IX","JEF","JMP","JZE","JEC","JRO","JRN","JKD","JHS","JKL","JHX","JGG","JFR","JPC","JBT","JHP","JZV","JTA","JZS","JPS","JKH","JNJ","JOE","JCG","JPG","JCE","JBR","JNY","JBN","JNS","JPZ","JOF","JKK","JZT","JPM","JZC","JLA","JBI","JAS","JCI","JZH","JLL","JTP","JZL","JKE","JKI","JKF","JRT","JGV","JKJ","JFP","JBO","JLS","JNPR","JBJ","JSN","JWF","JXI","JEQ","JAH","JZK","JBK","JBL","JTD","JYN","JWN","JAG","JKG","JHI","JZJ","JGT","JSM","JCP","JFC","JDD","JTX","JQC","KOP","KSU","KCP","KHI","KS","KRG","KVJ","KOS","KO","KEP","KRA","KEY","KST","KTV","KVN","KAR","KRO","KWK","KID","KEG","KCC","KBH","KED","KYO","KBR","KSM","KG","KCW","KDN","KCI","KND","KMX","KSS","KEI","KRH","KKD","KDE","KTH","KFS","KXI","KVW","KRJ","KSP","KVT","KNM","KLD","KWR","KTP","KNR","KBW","KF","KSK","KVF","KEX","KTF","KGC","KNX","KHD","KVU","KR","KFY","KTX","KCT","KAI","KT","KTC","KNO","KNL","KMT","KIM","KFN","KRC","KVR","KYN","KB","KTN","KGS","KFT","KUB","KMM","KOB","KEF","KMP","KMB","KOF","KSA","KMR","KYE","LGF","LDR","LXK","LMT","LTC","LUV","LTD","LVB","LII","LXP","LPS","LCM","LM","LNY","LEA","LOW","LLL","LDK","LFT","LDL","LYG","L","LOR","LPX","LUB","LBF","LTM","LH","LNT","LNN","LRY","LUK","LNC","LO","LAD","LSE","LHO","LGI","LXU","LG","LEE","LAB","LBY","LEG","LRN","LZ","LF","LL","LYV","LDF","LEO","LZB","LCC","LLY","LVS","LEN","LMI","LPL","LSI","LUX","LIZ","LFL","LAZ","LFC","MCN","MHF","MCY","MVO","MD","MSJ","MFV","MKL","MCO","MAS","MAN","MFB","MGB","MRO","MIM","MDZ","MLI","MCS","MED","MCR","MCI","MAC","MMC","MXT","MGF","MHI","MFD","MFM","MPJ","MCA","MGU","MSM","MBI","MS","MMR","MPS","MUE","MAT","MVL","MXB","MAR","MUH","MTG","MQY","MTP","MO","MDT","MTL","MGS","MIL","MAG","MSY","MYC","MYF","MWA","MLR","MMM","MMU","MFE","MZF","MRX","MDC","MMP","MSO","MPA","MYL","MHN","MYM","MHK","MTH","MHD","MHS","MFW","MTE","MTX","MHO","MF","MKS","MDS","MLG","MCK","MEI","MAY","MFA","MR","MSD","MTW","ME","MYI","MSZ","MTN","MDR","MLM","MSK","MNE","MOD","MMS","MEN","MRF","MIN","MUJ","MNI","MVC","MRK","MOV","MYN","MHY","MCD","MER","MTB","MJY","MFT","MUR","MWV","MBT","MHP","MJF","MPW","MXI","MYJ","MYD","MJV","MPG","MQT","MJT","MT","MIR","MAD","MTU","MWE","MIC","MFC","MXF","MYE","MVT","MON","MAA","MOT","MPR","MWO","MNP","MGM","MWG","MUA","MTT","MA","MAV","MSF","MEG","MWW","MUI","MDG","MDU","MTZ","MIG","MSA","MKC","MJH","MUC","MU","MPX","MLP","MOS","MOH","MEE","MI","MRT","MIY","MJI","MGA","MJN","MFL","MSP","MTR","MW","MUS","MMT","MXE","MRH","MPV","MKV","MTD","MFG","MET","MGI","MG","MSB","MWR","MTA","MDP","MTS","NCV","NQU","NFG","NCP","NCR","NSM","NPD","NUV","NVO","NE","NYC","NRN","NSL","NJ","NTE","NS","NFX","NMR","NAV","NED","NVS","NRP","NNC","NRT","NAT","NBR","NTY","NM","NPM","NIO","NLC","NAL","NMI","NXY","NUE","NNF","NRU","NCT","NNA","NRG","NST","NRF","NQI","NXP","NCZ","NFJ","NX","NVC","NMM","NPI","NUM","NMO","NEU","NU","NEV","NMY","NI","NQS","NC","NUW","NNJ","NGZ","NYT","N","NPP","NIF","NMD","NNY","NLY","NGT","NGS","NOA","NIM","NUO","NNI","NP","NCA","NNN","NYM","NPV","NVR","NFP","NOK","NPY","NWE","NCO","NSC","NUC","NMP","NWN","NQC","NUN","NTZ","NBG","NNP","NXN","NUS","NQM","NWY","NRC","NQJ","NIE","NMA","NCL","NWL","NYB","NMT","NXC","NTX","NOV","NGLS","NPX","NL","NZ","NAN","NQN","NVE","NQP","NZT","NYX","NCS","NAC","NPT","NTC","NPK","NWS","NJR","NAI","NHP","NHS","NVN","NPF","NCI","NPC","NAD","NEM","NWK","NR","NGG","NHI","NBL","NDN","NSR","NTL","NY","NPO","NXR","NLS","NTT","NAZ","NSH","NKE","NXQ","NOC","OGE","ODC","OLA","OSG","OKS","OIL","OIS","OMN","OFC","OLN","OTE","ORA","OIA","OSK","OMC","OWW","OII","OXM","ORI","OZM","OMI","OIB","OFG","OHI","OMX","ODP","OKE","OC","ONB","OLP","OMG","OEH","OIC","OPY","OSM","OI","OCR","OME","OXY","ORB","ORN","OB","OCN","PRA","PKY","PVR","PYC","PCX","PJA","PL","PRF","PYB","PEO","PIF","PYK","PRX","PKH","PNF","PXD","PMO","PGI","PFL","PCS","PGR","PAY","PEI","PTY","POL","PNK","PNX","PHR","PBT","PLT","PYN","PMT","PZC","PNW","PJZ","PYM","PYY","PEG","PHI","PFD","PPM","PYV","PIS","PYO","PYJ","PSO","PFH","PHX","PSA","PRO","PLA","PAR","PTR","PGH","PPS","PYG","PGP","PXP","PEB","PX","PFE","PAI","PPG","PJC","PCP","PNR","PTP","PPO","PKE","PNI","PFO","PRE","PVG","PNU","PWR","PC","PAG","PSY","PRS","PJR","PVA","PHK","PZB","PAA","PFX","PKJ","PHG","PBY","PM","PIA","PMM","PRM","PCQ","PKK","PYA","PVD","PYE","PLL","PAC","PIY","PHD","PCL","PMG","PAS","PMF","PSW","PBH","PLP","PIM","PLV","PPD","PCH","PRD","PDA","PIR","PSS","PAM","PMI","PBI","PNH","PLD","PNY","PIJ","PFK","PVX","PKD","PUK","PTI","PYS","PJT","PFS","PYI","PHY","PGN","PHM","PRU","PWE","PSE","PDT","PCG","PPC","PVH","PHT","PKX","PDS","PJL","PJE","PG","PH","PSB","PDP","PNC","PDE","PBR","PLS","PHA","PKM","PII","PT","POH","PJJ","PEP","PQ","PHH","PCF","PPR","PCU","PFG","PTV","PYT","POR","PJS","PKG","POM","PTC","PYL","PMC","PIKE","PZN","PNM","POT","PKO","PML","PZE","PPL","PFN","PKI","PGM","PJI","PMX","PCK","PBG","PCN","PCM","QTM","QRR","QXM","RVT","RCC","RTI","RNR","RGA","ROB","RBC","RLI","RBS","RSO","RTN","RCS","RE","RPM","ROL","RGC","RQI","RAD","RSX","RAS","RBN","RSH","RLF","RMT","RT","REP","RDY","RHB","RDK","RST","RAH","RC","RGS","RSG","REG","RF","RCI","RVI","RHI","RRR","ROC","RJF","RXI","RMD","RBA","ROS","RSC","RZ","RISK","RAX","RHT","RTU","RAI","RA","RES","ROG","RRC","RTP","RDN","RYN","RS","RNP","RRD","RUK","ROP","RDC","RY","RWT","RLH","RIG","RNE","RRI","REV","RIT","RPF","RFI","RKT","RBV","ROK","RWF","RYJ","RCL","RYL","RX","RPT","RL","RGR","STC","SLA","SBR","SAY","SRZ","SRT","SLG","SLM","SYK","SRI","SKT","SNF","SCS","STZ","SIG","SCD","SKY","SJI","SF","SNN","SRX","SXC","SKH","SR","SYY","STR","SPA","SLE","SSS","SGK","SAN","SSP","SYX","SGL","SWM","SFN","SPH","STM","SWZ","SGF","SII","SBH","SHAW","SEP","SLT","SBG","SPC","SGY","SNX","SWS","SXL","SWY","STE","SUR","SJR","SWX","SFY","SD","SBX","SBS","SMG","SMP","SEL","SVR","SHO","SY","SBW","STRI","SYT","SSL","SHS","SAI","SCX","SGU","STN","STJ","STI","SMI","STL","SBP","SNE","SUN","SI","SM","SSI","SGA","SQM","SOR","SGZ","SMS","SLS","SEM","SKM","SFI","SKX","SPF","SMA","SFE","SSW","SKS","SNS","SUI","SPG","SAB","SHW","SCG","SFD","SPW","SEE","SFG","SHV","SXI","SYA","SPN","SJM","SVM","SRE","STV","SVN","SFL","SCI","STP","SNY","STO","SXE","SXT","SO","SJT","SNI","SAH","SWI","SOA","STD","SWC","SSD","SHI","SWN","SHG","SWK","SJW","SID","SB","STWD","STK","SAP","SOL","SNP","SPR","SLB","SUG","SNH","SLF","SVJ","SNA","STX","SVU","SEH","SRV","SAM","SUP","SAF","SON","SCR","SU","STU","STT","SCU","SPP","SLH","SNV","SE","SLW","SCL","TLM","TRF","TUC","TS","TG","TBL","TAC","TXT","TIE","TIF","THG","TVC","TSL","TAM","TTC","TKR","TWX","TPL","TKS","TRV","TTF","TNC","TZK","TOT","TEN","TMK","TAL","TUP","TGH","TYY","TLK","TXN","TM","TGT","TKC","TSO","TLP","TMM","TEG","TWI","TGP","TRN","TIN","TV","TCH","TDA","TFC","TEI","TDI","TRC","TVL","TRS","THO","TC","TSU","TIP","TYW","TEL","TKF","TLB","THC","TOO","TSP","TEX","TAP","TD","THI","TLH","TRW","TMH","TMX","TEO","TBH","TWC","TCI","TRK","TSI","TSS","TNP","TPC","TWN","THS","TK","TYG","TRA","TE","TNL","TDC","TTI","TRU","TW","TEF","TTM","TCL","TDS","TGS","TYN","TVE","TCM","TCK","TBI","TCB","TREX","TX","TLI","TU","TZF","TTO","TYL","TYC","TNK","TR","TSN","TAI","TPX","TFX","TSM","TOL","TMR","TDF","TMO","TPZ","TGI","TNH","TRR","TXI","TJX","TII","TDG","TMS","TRP","TGX","TNE","TRH","TOD","TRI","TY","TAR","TDW","TI","TCO","TER","TNS","TNB","TDY","UDR","UFS","URS","UGI","UNS","UTL","UPL","UZV","UIS","UBA","UNP","UVV","USG","UNF","UTI","USU","UAM","UMC","UA","UNT","UBS","UIL","UL","UGP","UFI","UHS","UTR","USB","USA","UBP","UST","URI","UTF","UN","UTA","UPS","USM","UHT","UNH","UNM","UTX","UZG","VBF","VIM","VGR","VCI","VMO","VALE","VAR","VTR","VCO","VTJ","VIA","VR","VTA","VOQ","VHI","VRX","VZ","VMC","VTN","VCV","VLO","VNR","VVR","VLT","VM","VNO","VOL","VPV","V","VVC","VVI","VOD","VRS","VKQ","VGM","VFC","VAL","VIP","VMW","VE","VNV","VIT","VLY","VNOD","VSH","VSI","VQ","VMI","VIV","VG","WTR","WHX","WDR","WHI","WLL","WES","WTS","WFR","WRB","WFC","WFT","WTI","WMZ","WWW","WIW","WRE","WBK","WBC","WG","WST","WIN","WMG","WMK","WNS","WRS","WBD","WCC","WHR","WTM","WNR","WF","WCN","WDC","WYN","WSM","WH","WIT","WTW","WEC","WMS","WAT","WHG","WEN","WM","WRD","WW","WWY","WSH","WTU","WWE","WLP","WMT","WNC","WPK","WAB","WNI","WRI","WAL","WLT","WXS","WPI","WSF","WR","WPZ","WLK","WIA","WRC","WL","WSO","WGL","WAG","WX","WPP","WCG","WY","WU","WCO","WPS","WMB","WOR","WGO","WBS","WPO","WEA","WPC","XL","XFJ","XCO","XVG","XFB","XOM","XKN","XVF","XTO","XJT","XAA","XFP","XKO","XKK","XEC","XRX","XFL","XIN","XFH","XKE","XCJ","XRM","XEL","XFD","XFR","ZTR","ZZ","ZEP","ZLC","ZNT","ZF","ZNH","ZAP","ZQK","ZMH"};
    
    public final int index;

    protected final Session session;
    protected static volatile Double nextGaussian = null;

    public Operation(int idx)
    {
        index = idx;
        session = Pricer.session;
    }

    /**
     * Run operation
     * @param client Cassandra Thrift client connection
     * @throws IOException on any I/O error.
     */
    public abstract void run(Cassandra.Client client) throws IOException;

    // Utility methods

    /**
     * Generate values of average size specified by -S, up to cardinality specified by -C
     * @return Collection of the values
     */
    protected List<String> generateValues()
    {
        List<String> values = new ArrayList<String>();

        int limit = 2 * session.getColumnSize();

        for (int i = 0; i < session.getCardinality(); i++)
        {
            byte[] value = new byte[Pricer.randomizer.nextInt(limit)];
            Pricer.randomizer.nextBytes(value);

            values.add(FBUtilities.bytesToHex(value));
        }

        return values;
    }

    
    protected String[] generatePortfolio(int limit)
    {
        String[] portfolio = new String[Pricer.randomizer.nextInt(limit)+1];
        
        for(int i=0; i<portfolio.length; i++)
        {
            portfolio[i] = pickStock();
        }
        
        return portfolio;
    }
    
    protected String pickStock()
    {
        return tickers[Pricer.randomizer.nextInt(tickers.length)];
    }
    
    /**
     * key generator using Gauss or Random algorithm
     * @return byte[] representation of the key string
     */
    protected static byte[] generateKey()
    {
        return (Pricer.session.useRandomGenerator()) ? generateRandomKey() : generateGaussKey();
    }

    /**
     * Random key generator
     * @return byte[] representation of the key string
     */
    private static byte[] generateRandomKey()
    {
        String format = "%0" + Pricer.session.getTotalKeysLength() + "d";
        return String.format(format, Pricer.randomizer.nextInt(Pricer.session.getNumKeys() - 1)).getBytes();
    }

    /**
     * Gauss key generator
     * @return byte[] representation of the key string
     */
    private static byte[] generateGaussKey()
    {
        Session session = Pricer.session;
        String format = "%0" + session.getTotalKeysLength() + "d";

        for (;;)
        {
            double token = nextGaussian(session.getMean(), session.getSigma());

            if (0 <= token && token < session.getNumKeys())
            {
                return String.format(format, (int) token).getBytes();
            }
        }
    }

    /**
     * Gaussian distribution.
     * @param mu is the mean
     * @param sigma is the standard deviation
     *
     * @return next Gaussian distribution number
     */
    private static double nextGaussian(int mu, float sigma)
    {
        Random random = Pricer.randomizer;

        Double currentState = nextGaussian;
        nextGaussian = null;

        if (currentState == null)
        {
            double x2pi  = random.nextDouble() * 2 * Math.PI;
            double g2rad = Math.sqrt(-2.0 * Math.log(1.0 - random.nextDouble()));

            currentState = Math.cos(x2pi) * g2rad;
            nextGaussian = Math.sin(x2pi) * g2rad;
        }

        return mu + currentState * sigma;
    }

    /**
     * MD5 string generation
     * @param input String
     * @return md5 representation of the string
     */
    private String getMD5(String input)
    {
        MessageDigest md = FBUtilities.threadLocalMD5Digest();
        byte[] messageDigest = md.digest(input.getBytes());
        StringBuilder hash = new StringBuilder(new BigInteger(1, messageDigest).toString(16));

        while (hash.length() < 32)
            hash.append("0").append(hash);

        return hash.toString();
    }

    /**
     * Equal to python/ruby - 's' * times
     * @param str String to multiple
     * @param times multiplication times
     * @return multiplied string
     */
    private String multiplyString(String str, int times)
    {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < times; i++)
            result.append(str);

        return result.toString();
    }

    protected String getExceptionMessage(Exception e)
    {
        String className = e.getClass().getSimpleName();
        String message = (e instanceof InvalidRequestException) ? ((InvalidRequestException) e).getWhy() : e.getMessage();
        return (message == null) ? "(" + className + ")" : String.format("(%s): %s", className, message);
    }

    protected void error(String message) throws IOException
    {
        if (!session.ignoreErrors())
            throw new IOException(message);
        else
            System.err.println(message);
    }
}
