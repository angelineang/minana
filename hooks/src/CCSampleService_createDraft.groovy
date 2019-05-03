/**
 * Created by MYANANG on 4/26/2019.
 */

import static com.mincom.base.InvalidArgumentException.validateNotNull;

public class SampleRecKey {
    private final String organisationCode;
    private final String laboratoryCode;
    private final String jobCode;
    private final String sampleCode;
    private final String sampleId;

    public SampleRecKey(String organisationCode, String laboratoryCode, String jobCode, String sampleCode, String sampleId) {
        validateNotNull(organisationCode, "organisationCode");
        validateNotNull(laboratoryCode, "laboratoryCode");
        validateNotNull(jobCode, "jobCode");
        validateNotNull(sampleCode, "sampleCode");
        this.organisationCode = organisationCode;
        this.laboratoryCode = laboratoryCode;
        this.jobCode = jobCode;
        this.sampleCode = sampleCode;
        this.sampleId = sampleId;
    }

    public String getOrganisationCode() {
        return organisationCode;
    }

    public String getLaboratoryCode() {
        return laboratoryCode;
    }

    public String getJobCode() {
        return jobCode;
    }

    public String getSampleCode() {
        return sampleCode;
    }

    public String getSampleId() {
        return sampleId;
    }
/*
    @Override
    public int hashCode() {
        int result = schemeCode.hashCode();
        result = 31 * result + analyteCode.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SchemeAnalyteKey{" +
                "schemeCode='" + schemeCode + '\'' +
                ", analyteCode='" + analyteCode + '\'' +
                '}';
    }*/
}

package com.abb.ventyx.test.cclas.m2000.service.ccsample

import com.google.common.collect.Lists
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.schemeversion.SchemeVersionKey
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTKey
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTRec
import com.mincom.ellipse.edoi.ejb.ccinstrumentgroupmember.CCINSTRUMENTGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.types.m2000.instances.CCSampleDTO
import com.mincom.eql.Query
import com.mincom.eql.QueryFactory
import com.mincom.eql.UpdateQuery
import com.mincom.eql.impl.UpdateQueryImpl
import org.apache.commons.lang3.StringUtils

import static com.mincom.cclas.query.QueryResultsCaster.getResult
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.base.util.TypeUtil.containsValue
import static java.util.Arrays.asList

public class CCSampleService_createDraft extends CoreServiceHook {
    private SampleRepository sampleRepository = getBean(SampleRepository.class);
    private TimedEDOIFacade edoiFacade = getBean(TimedEDOIFacade.class);

    @Override
    Object onPreExecute(Object inputs) {
        return null
    }

    private Set<CCSAMPLESCHEMERec> buildSourceSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode

        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sampleDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleTemplateCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    public Set<CCSAMPLESCHEMERec> buildDestSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode
        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(sampleDTO.getLaboratoryCode().getValue())));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    private Map<SchemeVersionKey, CCSAMPLESCHEMERec> buildSourceSVKeyAndSSRecMap(CCSampleDTO sampleDTO) {
        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap = new HashMap<>();

        Set<CCSAMPLESCHEMERec> sourceSSSet = buildSourceSSRecSet(sampleDTO);
        if (sourceSSSet == null) {
            return;
        }
        for (CCSAMPLESCHEMERec sourceSSRec : sourceSSSet) {
            SchemeVersionKey schemeVersionKey = toSchemeVersionKey(sourceSSRec)

            sourceSchemeVersionKeyAndSSRecMap.putIfAbsent(schemeVersionKey, sourceSSRec);
        }

        return sourceSchemeVersionKeyAndSSRecMap;
    }

    private SchemeVersionKey toSchemeVersionKey(CCSAMPLESCHEMERec sourceSSRec) {
        SchemeVersionKey schemeVersionKey = new SchemeVersionKey(
                sourceSSRec.getOrganisationCode(),
                sanitiseCode(sourceSSRec.getSchemeLaboratoryCode()),
                sourceSSRec.getSchemeCode(),
                sourceSSRec.getSchemeVersionNumber());
        schemeVersionKey
    }


    @Override
    Object onPostExecute(Object input, Object result, Object returnWarnings) {

        CCSampleDTO sampleDTO = input as CCSampleDTO;

        if (sampleDTO == null) {
            return null;
        }

        if (!containsValue(sampleDTO.getLaboratoryCode())) {
            return result
        }

        Set<CCSAMPLESCHEMERec> destLatestSSRecSet = buildDestSSRecSet(sampleDTO);
        if (destLatestSSRecSet == null) {
            return result;
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSVKeyAndSourceSSRecMap = buildSourceSVKeyAndSSRecMap(sampleDTO);

        List<UpdateQuery> ssUpdateQueries = new ArrayList<>();
        for (CCSAMPLESCHEMERec latestSSRec : destLatestSSRecSet) {

            Query query = getLatestSSRecToUpdateQuery(latestSSRec)
            CCSAMPLESCHEMERec sourceSSRec = sourceSVKeyAndSourceSSRecMap.get(toSchemeVersionKey(latestSSRec));

            if (sourceSSRec == null) {
                throw new RuntimeException("Unable to locate source SSRec based on schemeVersionKey. This must be dev's bug.");
            }

            String instrumentCode = null;
            String instrumentLabCode = null;
            if (!containsValue(sourceSSRec.getInstrumentCode())) {
                String[] results = getInstrumentRecViaInstrumentGroup(sourceSSRec)

                if (results != null && results.length == 2) {
                    instrumentCode = results[0]
                    instrumentLabCode = results[1]
                }
            }

            UpdateQuery updateQuery = new UpdateQueryImpl(query);

            updateQuery.set(CCSAMPLESCHEMERec.instrumentLabCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentLabCode()) ? sourceSSRec.getInstrumentLabCode() : instrumentLabCode))
            updateQuery.set(CCSAMPLESCHEMERec.instrumentCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentCode()) ? sourceSSRec.getInstrumentCode() : instrumentCode))

            ssUpdateQueries.add(updateQuery);
        }

        if (!ssUpdateQueries.isEmpty()) {
            edoiFacade.updateAllBatch(ssUpdateQueries);
        }

        return result
    }

    private String[] getInstrumentRecViaInstrumentGroup(CCSAMPLESCHEMERec sourceSSRec) {
        Query instrumentCodeQuery = QueryFactory.query(CCINSTRUMENTRec.class);
        instrumentCodeQuery
                .columns(asList(CCINSTRUMENTRec.code, CCINSTRUMENTRec.laboratoryCode))
                .and(CCSCHEMERec.instrumentGroupId.equalTo(CCINSTRUMENTGROUPMEMBERRec.instrumentGroupId))
                .and(CCINSTRUMENTGROUPMEMBERRec.instrumentId.equalTo(CCINSTRUMENTKey.id))
                .and(CCSCHEMERec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCSCHEMERec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCSCHEMERec.code.equalTo(sourceSSRec.getSchemeCode()))
                .and(CCINSTRUMENTRec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCINSTRUMENTRec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCINSTRUMENTRec.isActive.equalTo(YES))
                .orderByAscending(CCINSTRUMENTGROUPMEMBERRec.aix2)
                .setMaxResults(1)

        String[] result = getResult(edoiFacade.scroll(instrumentCodeQuery));
        result
    }

    private Query getLatestSSRecToUpdateQuery(CCSAMPLESCHEMERec latestSSRec) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(latestSSRec.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(latestSSRec.getLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(latestSSRec.getJobCode()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(latestSSRec.getSampleCode()));
        query.and(CCSAMPLESCHEMERec.schemeLaboratoryCode.equalTo(sanitiseCode(latestSSRec.getSchemeLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.schemeCode.equalTo(latestSSRec.getSchemeCode()));
        query.and(CCSAMPLESCHEMERec.schemeVersionNumber.equalTo(latestSSRec.getSchemeVersionNumber()));
        query
    }

    private String sanitiseCode(String value) {
        if (StringUtils.isBlank(value)) {
            return NULL_VALUE;
        } else {
            return value;
        }
    }
}
import com.google.common.collect.Lists
import com.mincom.cclas.domain.job.JobRecKey
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.schemeversion.SchemeVersionKey
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.cclas.util.SystemConstants
import com.mincom.cclas.util.rec.RecKeyUtil
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTKey
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTRec
import com.mincom.ellipse.edoi.ejb.ccinstrumentgroupmember.CCINSTRUMENTGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLEKey
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLERec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.types.m2000.instances.CCSampleDTO
import com.mincom.eql.Query
import com.mincom.eql.QueryFactory
import com.mincom.eql.UpdateQuery
import com.mincom.eql.impl.UpdateQueryImpl
import org.apache.commons.lang3.StringUtils

import static com.mincom.cclas.query.QueryResultsCaster.getResult
import static com.mincom.cclas.type.util.UIDUtil.convertToStringSet
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.base.util.TypeUtil.containsValue
import static java.util.Arrays.asList

public class CCSampleService_multipleAddSchemesFromSamples extends CoreServiceHook {
    //This thread-local based map will be used to keep differentiate new SampleSchemes that are added into Samples.
    private final
    static ThreadLocal<Map<String, Set<CCSAMPLESCHEMERec>>> SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL = new ThreadLocal<>();

    private SampleRepository sampleRepository = getBean(SampleRepository.class);
    private TimedEDOIFacade edoiFacade = getBean(TimedEDOIFacade.class);

    @Override
    Object onPreExecute(Object inputs) {
        //always reset it first
        SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.set(null);

        CCSampleDTO[] sampleDTOS = inputs as CCSampleDTO[];

        if (sampleDTOS == null || sampleDTOS.length == 0) {
            return null;
        }

        Map<String, Set<CCSAMPLESCHEMERec>> destSampleIdAndSSRecMap = buildDestSampleIdAndSSRecMap(sampleDTOS);
        SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.set(destSampleIdAndSSRecMap);

        return null
    }

    private Map<String, Set<CCSAMPLESCHEMERec>> buildDestSampleIdAndSSRecMap(CCSampleDTO[] sampleDTOS) {
        Set<String> destinationSampleIds = new HashSet<>();
        for (CCSampleDTO sampleDTO : sampleDTOS) {
            if (sampleDTO.getSampleIds() != null) {
                destinationSampleIds.addAll(convertToStringSet(sampleDTO.getSampleIds()));
            }
        }

        if (destinationSampleIds.isEmpty()) {
            return null;
        }

        JobRecKey destinationJobKey = findDestinationJobKey(destinationSampleIds)

        return buildSampleIdAndSSRecMap(destinationJobKey, destinationSampleIds);
    }

    private Map<String, Set<CCSAMPLESCHEMERec>> buildSampleIdAndSSRecMap(JobRecKey jobRecKey, Set<String> requiredSampleIds) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode

        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(jobRecKey.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(jobRecKey.getLaboratoryCode()));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(jobRecKey.getCode()));

        Map<String, Set<CCSAMPLESCHEMERec>> map = new HashMap<>();

        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();

            //skip those samples that are not in the destination sampleIds list
            if (!requiredSampleIds.contains(ssRec.getSampleId())) {
                continue;
            }

            Set<CCSAMPLESCHEMERec> ssSet = map.get(ssRec.getSampleId());
            if (ssSet == null) {
                ssSet = new HashSet<>();
                map.put(ssRec.getSampleId(), ssSet);
            }
            ssSet.add(ssRec);
        }

        return map;
    }

    private JobRecKey findDestinationJobKey(Set<String> destinationSampleIds) {
        //since destination SampleIds always come from the same job, so it is safe to assume all destination sampleIds are belong to a same job.
        String firstSampleId = destinationSampleIds.iterator().next();

        List<CCSAMPLERec> sampleRecs = sampleRepository.loadSparseRecs(Lists.asList(firstSampleId),
                CCSAMPLEKey.id,
                CCSAMPLERec.organisationCode,
                CCSAMPLERec.laboratoryCode,
                CCSAMPLERec.jobCode,
                CCSAMPLERec.code);

        if (sampleRecs.isEmpty()) {
            throw new RuntimeException("Unable to locale jobCode for the given sampleId:" + firstSampleId);
        }

        CCSAMPLERec sampleRec = sampleRecs.get(0);
        JobRecKey jobKey = new JobRecKey(sampleRec.getOrganisationCode(), sanitiseCode(sampleRec.getLaboratoryCode()), sampleRec.getJobCode())
        jobKey
    }

    private Map<JobRecKey, Set<String>> buildJobRecKeyAndSampleIdsMap(CCSampleDTO[] sampleDTOS) {
        Map<JobRecKey, Set<String>> map = new HashMap<>();

        for (CCSampleDTO sampleDTO : sampleDTOS) {
            JobRecKey jobKey = new JobRecKey(sampleDTO.getOrganisationCode().getValue(),
                    sanitiseCode(sampleDTO.getLaboratoryCode().getValue()), sampleDTO.getJobCode().value);

            Set<String> sampleIds = map.get(jobKey);
            if (sampleIds == null) {
                sampleIds = new HashSet<>();
                map.put(jobKey, sampleIds);
            }
            sampleIds.add(sampleDTO.getSampleId().getValue());
        }

        return map;
    }

    private Map<SchemeVersionKey, CCSAMPLESCHEMERec> buildSourceSchemeVersionKeyAndSSRecMap(CCSampleDTO[] sampleDTOS) {
        Map<JobRecKey, Set<String>> sourceJobRecKeyAndSampleIdsMap = buildJobRecKeyAndSampleIdsMap(sampleDTOS);

        Map<String, Set<CCSAMPLESCHEMERec>> sourceSampleIdAndSSMap = new HashMap<>();

        for (Map.Entry<JobRecKey, Set<String>> entry : sourceJobRecKeyAndSampleIdsMap.entrySet()) {
            JobRecKey sourceJobKey = entry.getKey();
            Set<String> sourceSampleIds = entry.getValue();
            sourceSampleIdAndSSMap.putAll(buildSampleIdAndSSRecMap(sourceJobKey, sourceSampleIds));
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap = new HashMap<>();
        //if multiple sources of samples have the same scheme and schemeVersion, the first sampleDTO will take precedence.
        for (CCSampleDTO sampleDTO : sampleDTOS) {
            Set<CCSAMPLESCHEMERec> sourceSSMap = sourceSampleIdAndSSMap.get(sampleDTO.getSampleId().getValue());

            if (sourceSSMap == null) {
                continue;
            }

            for (CCSAMPLESCHEMERec sourceSSRec : sourceSSMap) {
                SchemeVersionKey schemeVersionKey = toSchemeVersionKey(sourceSSRec)

                sourceSchemeVersionKeyAndSSRecMap.putIfAbsent(schemeVersionKey, sourceSSRec);
            }
        }

        return sourceSchemeVersionKeyAndSSRecMap;
    }

    private SchemeVersionKey toSchemeVersionKey(CCSAMPLESCHEMERec sourceSSRec) {
        SchemeVersionKey schemeVersionKey = new SchemeVersionKey(
                sourceSSRec.getOrganisationCode(),
                sanitiseCode(sourceSSRec.getSchemeLaboratoryCode()),
                sourceSSRec.getSchemeCode(),
                sourceSSRec.getSchemeVersionNumber());
        schemeVersionKey
    }


    @Override
    Object onPostExecute(Object inputs, Object result, Object returnWarnings) {

        Map<String, Set<CCSAMPLESCHEMERec>> destSampleIdAndOriginalSSRecMap = SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.get();
        //skip it if the map is not set
        if (destSampleIdAndOriginalSSRecMap == null) {
            return result;
        }

        CCSampleDTO[] sampleDTOS = inputs as CCSampleDTO[];

        if (!containsValue(sampleDTOS[0].getLaboratoryCode())) {
            return result
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap =
                buildSourceSchemeVersionKeyAndSSRecMap(sampleDTOS);


        Map<String, Set<CCSAMPLESCHEMERec>> destSampleIdAndLatestSSRecMap = buildDestSampleIdAndSSRecMap(sampleDTOS);

        List<UpdateQuery> ssUpdateQueries = new ArrayList<>();

        for (Map.Entry<String, Set<CCSAMPLESCHEMERec>> entry : destSampleIdAndLatestSSRecMap) {
            String destSampleId = entry.getKey();
            Set<CCSAMPLESCHEMERec> destLatestSSRecSet = entry.getValue();

            Set<CCSAMPLESCHEMERec> destOriginalSSRecSet = destSampleIdAndOriginalSSRecMap.get(destSampleId);

            for (CCSAMPLESCHEMERec latestSSRec : destLatestSSRecSet) {
                //check whether it is a newly added SS, if so, we need to update the instrumentCode
                if (destOriginalSSRecSet == null || !destOriginalSSRecSet.contains(latestSSRec)) {

                    Query query = getLatestSSRecQuery(latestSSRec)

                    CCSAMPLESCHEMERec sourceSSRec = sourceSchemeVersionKeyAndSSRecMap.get(toSchemeVersionKey(latestSSRec));
                    if (sourceSSRec == null) {
                        throw new RuntimeException("Unable to locate source SSRec based on schemeVersionKey. This must be dev's bug.");
                    }

                    String instrumentCode = null;
                    String instrumentLabCode = null;
                    if (!containsValue(sourceSSRec.getInstrumentCode())) {
                        String [] results = getInstrumentRecViaInstrumentGroup(sourceSSRec)

                        if (results != null && results.length == 2) {
                            instrumentCode = results[0]
                            instrumentLabCode = results[1]
                        }
                    }

                    UpdateQuery updateQuery = new UpdateQueryImpl(query);

                    updateQuery.set(CCSAMPLESCHEMERec.instrumentLabCode,
                            sanitiseCode(containsValue(sourceSSRec.getInstrumentLabCode()) ? sourceSSRec.getInstrumentLabCode() : instrumentLabCode))
                    updateQuery.set(CCSAMPLESCHEMERec.instrumentCode,
                            sanitiseCode(containsValue(sourceSSRec.getInstrumentCode()) ? sourceSSRec.getInstrumentCode() : instrumentCode))

                    ssUpdateQueries.add(updateQuery);
                }
            }
        }

        if (!ssUpdateQueries.isEmpty()) {
            edoiFacade.updateAllBatch(ssUpdateQueries);
        }

        SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.set(null);
        return result
    }

    private Query getLatestSSRecQuery(CCSAMPLESCHEMERec latestSSRec) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(latestSSRec.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(latestSSRec.getLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(latestSSRec.getJobCode()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(latestSSRec.getSampleCode()));
        query.and(CCSAMPLESCHEMERec.schemeLaboratoryCode.equalTo(sanitiseCode(latestSSRec.getSchemeLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.schemeCode.equalTo(latestSSRec.getSchemeCode()));
        query.and(CCSAMPLESCHEMERec.schemeVersionNumber.equalTo(latestSSRec.getSchemeVersionNumber()));
        query
    }


    private String[] getInstrumentRecViaInstrumentGroup(CCSAMPLESCHEMERec sourceSSRec) {
        Query instrumentCodeQuery = QueryFactory.query(CCINSTRUMENTRec.class);
        instrumentCodeQuery
                .columns(asList(CCINSTRUMENTRec.code, CCINSTRUMENTRec.laboratoryCode))
                .and(CCSCHEMERec.instrumentGroupId.equalTo(CCINSTRUMENTGROUPMEMBERRec.instrumentGroupId))
                .and(CCINSTRUMENTGROUPMEMBERRec.instrumentId.equalTo(CCINSTRUMENTKey.id))
                .and(CCSCHEMERec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCSCHEMERec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCSCHEMERec.code.equalTo(sourceSSRec.getSchemeCode()))
                .and(CCINSTRUMENTRec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCINSTRUMENTRec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCINSTRUMENTRec.isActive.equalTo(YES))
                .orderByAscending(CCINSTRUMENTGROUPMEMBERRec.aix2)
                .setMaxResults(1)

        String[] result = getResult(edoiFacade.scroll(instrumentCodeQuery));
        result
    }

    private String sanitiseCode(String value){
        if(StringUtils.isBlank(value)){
            return NULL_VALUE;
        }else{
            return value;
        }
    }
}
import com.mincom.cclas.domain.job.JobRecKey
import com.mincom.cclas.domain.job.JobRepository
import com.mincom.cclas.domain.job.scheme.JobSchemeRepository
import com.mincom.cclas.domain.reporttemplate.ReportTemplateReportTypeEnum
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.sample.scheme.SampleSchemeRepository
import com.mincom.cclas.impl.domain.MessageUtil
import com.mincom.cclas.impl.domain.reportrequest.builder.generator.ReportGenerator
import com.mincom.cclas.query.FastUpdateQuery
import com.mincom.cclas.query.NormalizedConstraintBuilder
import com.mincom.cclas.query.QueryUtil
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.cclas.report.request.ReportRequestBuilder
import com.mincom.cclas.report.request.ReportRequestOutput
import com.mincom.cclas.report.request.ReportRequestService
import com.mincom.cclas.report.request.ReportableRule
import com.mincom.cclas.security.SecurityContextService
import com.mincom.ellipse.app.security.SecurityToken
import com.mincom.ellipse.edoi.ejb.QueryResults
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.cccategory.CCCATEGORYKey
import com.mincom.ellipse.edoi.ejb.cccategory.CCCATEGORYRec
import com.mincom.ellipse.edoi.ejb.ccdevice.CCDEVICEKey
import com.mincom.ellipse.edoi.ejb.ccdevice.CCDEVICERec
import com.mincom.ellipse.edoi.ejb.ccjobscheme.CCJOBSCHEMERec
import com.mincom.ellipse.edoi.ejb.ccqctype.CCQCTYPEKey
import com.mincom.ellipse.edoi.ejb.ccqctype.CCQCTYPERec
import com.mincom.ellipse.edoi.ejb.ccreporttemplate.CCREPORTTEMPLATEKey
import com.mincom.ellipse.edoi.ejb.ccreporttemplate.CCREPORTTEMPLATERec
import com.mincom.ellipse.edoi.ejb.ccrepreqsample.CCREPREQSAMPLERec
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLEKey
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLERec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.edoi.ejb.ccschemegroup.CCSCHEMEGROUPKey
import com.mincom.ellipse.edoi.ejb.ccschemegroup.CCSCHEMEGROUPRec
import com.mincom.ellipse.edoi.ejb.ccschemegroupmember.CCSCHEMEGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.msf0p5.MSF0P5Rec
import com.mincom.ellipse.errors.Error
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.security.SecurityTokenServiceLocator
import com.mincom.ellipse.service.ServiceResult
import com.mincom.ellipse.service.m2000.ccjob.CCJobService
import com.mincom.ellipse.service.m2000.ccjobpaperwork.CCJobPaperworkService
import com.mincom.ellipse.service.m2000.ccreportrequest.CCReportRequestService
import com.mincom.ellipse.types.m2000.instances.*
import com.mincom.eql.Constraint
import com.mincom.eql.Query
import com.mincom.eql.StringConstraint
import com.mincom.eql.UpdateQuery
import com.mincom.eql.common.Rec
import com.mincom.eql.impl.QueryImpl
import com.mincom.eql.impl.StringConstraintImpl
import com.mincom.eql.impl.UpdateQueryImpl
import com.mincom.ji.cqrs.Messages
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static CCJobBatchService_multipleCreateValidations.canProcess
import static CCJobBatchService_multipleCreateValidations.validateHasJob
import static com.google.common.collect.Lists.newArrayList
import static com.google.common.collect.Sets.newHashSet
import static com.mincom.cclas.query.ConstraintUtil.equalAny
import static com.mincom.cclas.query.QueryResultsCaster.getResults
import static com.mincom.cclas.query.QueryUtil.alias
import static com.mincom.cclas.query.QueryUtil.andEqual
import static com.mincom.cclas.service.JIServiceUtil.locateJIService
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.m2000.types.AnalyticalTypeType.REPLICATE
import static com.mincom.ellipse.types.m2000.types.AnalyticalTypeType.DUPLICATE
import static com.mincom.ellipse.types.m2000.types.AnalyticalTypeType.UNKNOWN
import static com.mincom.eql.QueryFactory.query
import static java.util.Arrays.asList
import static org.apache.commons.lang.StringUtils.isNotBlank

/**
 * B-148436 (https://www11.v1host.com/VentyxProd/story.mvc/Summary?oidToken=Story%3A5985069)
 * Determine highest priority from samples in the lab batch, and set the batch priority to match this priority.
 * This is done so users can sort search results by priority, and since could be batching samples from any job,
 * the priority at the job level may not reflect the urgency from the sample level.
 *
 * B-138526 (https://www11.v1host.com/VentyxProd/story.mvc/Summary?oidToken=Story%3A5654415)
 * Determine the schemes in the batch and add as a comma separated list of schemes to the description of the batch job,
 * so users can see the schemes in the batch easily from workbook search.
 */
class CCJobBatchService_multipleCreate extends CoreServiceHook {

    protected final static String COMMA_DELIMITER = ",";
    protected final static String INST_BRACKET_START = "["
    protected final static String INST_BRACKET_END = "]"
    protected final static String SCH_BRACKET_START = "("
    protected final static String SCH_BRACKET_END = ")"
    protected static final String ALIAS_UNKNOWN_SAMPLE = "s";
    protected static final String ALIAS_QC_SAMPLE = "qc";

    private CCJobService srvJob
    private JobRepository repoJob
    private SampleRepository repoSample
    private JobSchemeRepository repoJobScheme
    private CCJobPaperworkService srvJobPW;
    private SecurityToken token
    private TimedEDOIFacade tEDOI

    private List<Boolean> autoCreatePaperwork

    private SampleSchemeRepository sampleSchemeRepository = getBean(SampleSchemeRepository.class);
    private JobRepository jobRepository = getBean(JobRepository.class);
    private SecurityContextService securityContextService = getBean(SecurityContextService.class);
    private ReportRequestService reportRequestServiceForRRBuilder = getBean(ReportRequestService.class);
    private CCReportRequestService reportRequestService = getBean(CCReportRequestService.class);
    private ReportGenerator reportGenerator = getBean(ReportGenerator.class);

    private Logger LOGGER = LoggerFactory.getLogger(CCJobBatchService_multipleCreate.class)

    protected CCJobBatchService_multipleCreate() {
        autoCreatePaperwork = []
    }

    protected CCJobBatchService_multipleCreate(JobRepository repoJob, JobSchemeRepository repoJobScheme, CCJobService srvJob, SecurityToken token, TimedEDOIFacade tEDOI, CCJobPaperworkService srvJobPW) {
        // Constructor used for test coverage
        this.srvJob = srvJob
        this.repoJob = repoJob;
        this.repoJobScheme = repoJobScheme;
        this.token = token
        this.tEDOI = tEDOI
        this.srvJobPW = srvJobPW

        autoCreatePaperwork = []
    }

    @Override
    Object onPreExecute(Object dto, Object returnWarnings) {
        // Disable automatic paperwork creation so we can generate the paperwork with the description created in the postExecute hook
        CCJobBatchSessionDTO[] dtoCast = (CCJobBatchSessionDTO[]) dto;
        dtoCast.eachWithIndex { CCJobBatchSessionDTO item, int index ->
            CCJobBatchCreateBatchDTO createBatchDTO = item.getJobBatchCreateBatchDTO()
            autoCreatePaperwork.add(index, createBatchDTO.getAutoCreatePaperwork().getValue())
            createBatchDTO.getAutoCreatePaperwork().setValue(false);
        }
        return null
    }

    @Override
    Object onPostExecute(Object input, Object result, Object returnWarnings) {
        if (!canProcess(result)) {
            return result
        }

        CCJobBatchSessionServiceResult[] resultsCast = (CCJobBatchSessionServiceResult[]) result;

        resultsCast.eachWithIndex { CCJobBatchSessionServiceResult entry, int i ->
            processReplicateResult(resultsCast[i], (Boolean) returnWarnings)
            processOneResult(resultsCast[i], (Boolean) returnWarnings, autoCreatePaperwork.get(i));
        }

        return result
    }

    private List<Object[]> findUnknownsWithPotentialClientQC(OrganisationCode orgCode, LaboratoryCode labCode, JobCode jobCode) {
        Query qcTypeQuery = getQcTypeQuery()

        Query unkSampleQuery = new QueryImpl(CCSAMPLERec.class, ALIAS_QC_SAMPLE)
                .rightJoin(CCSAMPLERec.dupOriginalSampleFK, ALIAS_UNKNOWN_SAMPLE)
                .and(alias(CCSAMPLERec.organisationCode, ALIAS_UNKNOWN_SAMPLE).equalTo(orgCode.getValue()))
                .and(alias(CCSAMPLERec.laboratoryCode, ALIAS_UNKNOWN_SAMPLE).equalTo(labCode.getValue()))
                .and(alias(CCSAMPLERec.jobCode, ALIAS_UNKNOWN_SAMPLE).equalTo(jobCode.getValue()))
                .and(alias(CCSAMPLERec.primaryAnalyticalType, ALIAS_UNKNOWN_SAMPLE).equalTo(UNKNOWN))
                .and(alias(CCSAMPLERec.secondaryQcTypeId, ALIAS_UNKNOWN_SAMPLE).in(qcTypeQuery, CCQCTYPEKey.id)
                .or(alias(CCSAMPLERec.primaryAnalyticalType, ALIAS_QC_SAMPLE).equalTo(REPLICATE)))
                .noAutoI18n()
                .asEntity()

        return getResults(getTimedEDOIFacade().search(unkSampleQuery));
    }


    private List<CCSAMPLERec> queryPotentialUnknowns(OrganisationCode orgCode, LaboratoryCode labCode, JobCode jobCode,
                                                     Set<String> nonPotentialUnknowns, int maxSize) {

        Query unkSampleQuery = new QueryImpl(CCSAMPLERec.class)
                .and(CCSAMPLERec.organisationCode.equalTo(orgCode.getValue()))
                .and(CCSAMPLERec.laboratoryCode.equalTo(labCode.getValue()))
                .and(CCSAMPLERec.jobCode.equalTo(jobCode.getValue()))
                .and(CCSAMPLERec.primaryAnalyticalType.equalTo(UNKNOWN))
                .and(CCSAMPLERec.code.notIn(nonPotentialUnknowns))
                .noAutoI18n()
                .setMaxResults(maxSize)
                .asEntity()

        return getResults(getTimedEDOIFacade().search(unkSampleQuery));
    }

    private static Query getQcTypeQuery() {
        Query qcTypeQuery = query(CCQCTYPERec.class)
                .and(CCQCTYPERec.qcTypeCategoryId.equalTo(CCCATEGORYKey.id))
                .and(CCCATEGORYRec.code.equalTo('SECONDARY'))
        qcTypeQuery
    }


    CCJobBatchSessionServiceResult processReplicateResult(CCJobBatchSessionServiceResult result, Boolean returnWarnings) {
        if (!canProcess(result)) {
            return result;
        }
        Messages messages = new Messages();
        MessageUtil.copyMessages(messages, result);

        CCJobBatchSessionDTO jobBatchDTO = result.getCCJobBatchSessionDTO();
        List<Object[]> recs = findUnknownsWithPotentialClientQC(jobBatchDTO.getOrganisationCode(), jobBatchDTO.getLaboratoryCode(),
                jobBatchDTO.getJobCode());

        Set<String> nonPotentialUnknowns = new HashSet<>();
        LinkedHashMap<SampleRecKey, Set<CCSAMPLERec>> unknownWithClientQcMap = getUnknownWithClientQC(recs, nonPotentialUnknowns)

        List<CCSAMPLERec> pontentialNewUnknowns = queryPotentialUnknowns(jobBatchDTO.getOrganisationCode(), jobBatchDTO.getLaboratoryCode(),
                jobBatchDTO.getJobCode(), nonPotentialUnknowns, unknownWithClientQcMap.size());

        FastUpdateQuery updateQuery = getFastUpdateQueryToUpdateSample()

        for (int i = 0; i < pontentialNewUnknowns.size(); i++) {
            CCSAMPLERec newPontentialUnknown = pontentialNewUnknowns.get(i);
            Set<CCSAMPLERec> movableRepInUnknown = (new ArrayList<Set<CCSAMPLERec>>(unknownWithClientQcMap.values())).get(i)// -- i unknown
            for (CCSAMPLERec movableRepRec : movableRepInUnknown) {
                Object[] updateValues = [newPontentialUnknown.getPrimaryKey().getId(),
                                         newPontentialUnknown.getName(),
                                         newPontentialUnknown.getDescription(),
                                         newPontentialUnknown.getClientSampleName()]

                Object[] paramValues = [movableRepRec.getOrganisationCode(),
                                        movableRepRec.getLaboratoryCode(),
                                        movableRepRec.getJobCode(),
                                        movableRepRec.getCode()]

                updateQuery.addRow(updateValues, paramValues);
            }
        }
        getTimedEDOIFacade().executeUpdate(updateQuery);

        return result;
    }

    private LinkedHashMap<SampleRecKey, Set<CCSAMPLERec>> getUnknownWithClientQC(List<Object[]> recs, HashSet<String> nonPotentialUnknowns) {
        LinkedHashMap<SampleRecKey, Set<CCSAMPLERec>> unknownWithClientQcMap = new LinkedHashMap<>();

        for (Object[] rec : recs) {
            CCSAMPLERec[] values = (CCSAMPLERec[]) rec;
            CCSAMPLERec unknown = values[1];
            nonPotentialUnknowns.add(unknown.getCode())

            if (values[0] != null) {
                CCSAMPLERec qc = values[0];

                if (isClientQC(unknown, qc)) {
                    SampleRecKey sampleKey = toSampleKey(unknown.getOrganisationCode(),
                            unknown.getLaboratoryCode(),
                            unknown.getJobCode(),
                            unknown.getCode(),
                            unknown.getPrimaryKey().getId())
                    Set<CCSAMPLERec> sSet = unknownWithClientQcMap.get(sampleKey);

                    if (sSet == null) {
                        sSet = new HashSet<>();
                        unknownWithClientQcMap.put(sampleKey, sSet);
                    }

                    sSet.add(qc);
                }
            }
        }
        return unknownWithClientQcMap;
    }

    private FastUpdateQuery getFastUpdateQueryToUpdateSample() {
        return new FastUpdateQuery(
                CCSAMPLERec.class,
                newArrayList(CCSAMPLERec.dupOriginalSampleId,
                        new StringConstraintImpl(CCSAMPLERec.class, "name", 40, true, false).columnName("NAME") as StringConstraint,
                        CCSAMPLERec.description,
                        CCSAMPLERec.clientSampleName
                ),
                newArrayList(
                        CCSAMPLERec.organisationCode,
                        CCSAMPLERec.laboratoryCode,
                        CCSAMPLERec.jobCode,
                        CCSAMPLERec.code
                )
        );
    }

    private boolean isClientQC(CCSAMPLERec unknown, CCSAMPLERec qc) {
        unknown.getSecondaryQcTypeId() != null && qc.getPrimaryAnalyticalType() == REPLICATE
    }

    private SampleRecKey toSampleKey(String orgCode, String labCode, String jobCode, String sampleCode, String sampleId) {
        SampleRecKey sampleKey = new SampleRecKey(orgCode, labCode, jobCode, sampleCode, sampleId);
        sampleKey
    }

    CCJobBatchSessionServiceResult processOneResult(CCJobBatchSessionServiceResult result, Boolean returnWarnings, Boolean paperwork) {
        if (!canProcess(result)) {
            return result;
        }

        Messages messages = new Messages();
        MessageUtil.copyMessages(messages, result);

        // TODO: Is it important to return Ii8N?
        CCJobBatchSessionDTO jobBatchDTO = result.getCCJobBatchSessionDTO();
        CCJobDTO jobDTO = getJobRepository().findByCode(jobBatchDTO.getJobCode(), jobBatchDTO.getOrganisationCode(), jobBatchDTO.getLaboratoryCode(), messages);
        if (messages.hasAnyErrors()) {
            MessageUtil.copyMessages(result, messages)
            return result
        }

        validateHasJob(jobDTO, jobBatchDTO, result)
        if (result.hasErrors()) {
            return result
        }

        updatePriorityDescriptionAndGeneratePaperwork(jobDTO, jobBatchDTO, messages, result, paperwork, returnWarnings)
        if (paperwork){
            // Change made as per B-157386; all paperwork should now honour batch dialog checkbox
            createSchemeGroupBasedPaperworkOrLabels(jobDTO, messages)
        }
        return result;
    }

    private Map<String, Set<String>> findSchemeGroupAndSchemesMap(Set<CCJOBSCHEMERec> jobSchemes) {
        Query query = query(CCSCHEMERec.class);

        query.and(CCSCHEMEKey.id.equalTo(CCSCHEMEGROUPMEMBERRec.schemeId));
        query.and(CCSCHEMEGROUPMEMBERRec.schemeGroupId.equalTo(CCSCHEMEGROUPKey.id));

        NormalizedConstraintBuilder schemeConstraintBuilder = new NormalizedConstraintBuilder();
        for (CCJOBSCHEMERec jobScheme : jobSchemes) {
            schemeConstraintBuilder.newEntry();

            schemeConstraintBuilder.setEqual(CCSCHEMERec.organisationCode, jobScheme.organisationCode);
            schemeConstraintBuilder.setEqual(CCSCHEMERec.laboratoryCode, jobScheme.schemeLaboratoryCode);
            schemeConstraintBuilder.setEqual(CCSCHEMERec.code, jobScheme.schemeCode);
        }

        query.and(schemeConstraintBuilder.buildConstraints());

        query.columns(newArrayList(CCSCHEMEGROUPRec.code, CCSCHEMERec.code));

        ScrollableResults scrollableResults = getTimedEDOIFacade().scroll(query);
        Map<String, Set<String>> schemeGroupAndSchemesMap = new HashMap<>();
        while (scrollableResults.hasNext()) {package com.abb.ventyx.test.cclas.m2000.service.ccsample

import com.google.common.collect.Lists
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.schemeversion.SchemeVersionKey
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTKey
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTRec
import com.mincom.ellipse.edoi.ejb.ccinstrumentgroupmember.CCINSTRUMENTGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.types.m2000.instances.CCSampleDTO
import com.mincom.eql.Query
import com.mincom.eql.QueryFactory
import com.mincom.eql.UpdateQuery
import com.mincom.eql.impl.UpdateQueryImpl
import org.apache.commons.lang3.StringUtils

import static com.mincom.cclas.query.QueryResultsCaster.getResult
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.base.util.TypeUtil.containsValue
import static java.util.Arrays.asList

public class CCSampleService_createDraft extends CoreServiceHook {
    private SampleRepository sampleRepository = getBean(SampleRepository.class);
    private TimedEDOIFacade edoiFacade = getBean(TimedEDOIFacade.class);

    @Override
    Object onPreExecute(Object inputs) {
        return null
    }

    private Set<CCSAMPLESCHEMERec> buildSourceSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode

        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sampleDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleTemplateCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    public Set<CCSAMPLESCHEMERec> buildDestSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode
        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(sampleDTO.getLaboratoryCode().getValue())));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    private Map<SchemeVersionKey, CCSAMPLESCHEMERec> buildSourceSVKeyAndSSRecMap(CCSampleDTO sampleDTO) {
        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap = new HashMap<>();

        Set<CCSAMPLESCHEMERec> sourceSSSet = buildSourceSSRecSet(sampleDTO);
        if (sourceSSSet == null) {
            return;
        }
        for (CCSAMPLESCHEMERec sourceSSRec : sourceSSSet) {
            SchemeVersionKey schemeVersionKey = toSchemeVersionKey(sourceSSRec)

            sourceSchemeVersionKeyAndSSRecMap.putIfAbsent(schemeVersionKey, sourceSSRec);
        }

        return sourceSchemeVersionKeyAndSSRecMap;
    }

    private SchemeVersionKey toSchemeVersionKey(CCSAMPLESCHEMERec sourceSSRec) {
        SchemeVersionKey schemeVersionKey = new SchemeVersionKey(
                sourceSSRec.getOrganisationCode(),
                sanitiseCode(sourceSSRec.getSchemeLaboratoryCode()),
                sourceSSRec.getSchemeCode(),
                sourceSSRec.getSchemeVersionNumber());
        schemeVersionKey
    }


    @Override
    Object onPostExecute(Object input, Object result, Object returnWarnings) {

        CCSampleDTO sampleDTO = input as CCSampleDTO;

        if (sampleDTO == null) {
            return null;
        }

        if (!containsValue(sampleDTO.getLaboratoryCode())) {
            return result
        }

        Set<CCSAMPLESCHEMERec> destLatestSSRecSet = buildDestSSRecSet(sampleDTO);
        if (destLatestSSRecSet == null) {
            return result;
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSVKeyAndSourceSSRecMap = buildSourceSVKeyAndSSRecMap(sampleDTO);

        List<UpdateQuery> ssUpdateQueries = new ArrayList<>();
        for (CCSAMPLESCHEMERec latestSSRec : destLatestSSRecSet) {

            Query query = getLatestSSRecToUpdateQuery(latestSSRec)
            CCSAMPLESCHEMERec sourceSSRec = sourceSVKeyAndSourceSSRecMap.get(toSchemeVersionKey(latestSSRec));

            if (sourceSSRec == null) {
                throw new RuntimeException("Unable to locate source SSRec based on schemeVersionKey. This must be dev's bug.");
            }

            String instrumentCode = null;
            String instrumentLabCode = null;
            if (!containsValue(sourceSSRec.getInstrumentCode())) {
                String[] results = getInstrumentRecViaInstrumentGroup(sourceSSRec)

                if (results != null && results.length == 2) {
                    instrumentCode = results[0]
                    instrumentLabCode = results[1]
                }
            }

            UpdateQuery updateQuery = new UpdateQueryImpl(query);

            updateQuery.set(CCSAMPLESCHEMERec.instrumentLabCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentLabCode()) ? sourceSSRec.getInstrumentLabCode() : instrumentLabCode))
            updateQuery.set(CCSAMPLESCHEMERec.instrumentCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentCode()) ? sourceSSRec.getInstrumentCode() : instrumentCode))

            ssUpdateQueries.add(updateQuery);
        }

        if (!ssUpdateQueries.isEmpty()) {
            edoiFacade.updateAllBatch(ssUpdateQueries);
        }

        return result
    }

    private String[] getInstrumentRecViaInstrumentGroup(CCSAMPLESCHEMERec sourceSSRec) {
        Query instrumentCodeQuery = QueryFactory.query(CCINSTRUMENTRec.class);
        instrumentCodeQuery
                .columns(asList(CCINSTRUMENTRec.code, CCINSTRUMENTRec.laboratoryCode))
                .and(CCSCHEMERec.instrumentGroupId.equalTo(CCINSTRUMENTGROUPMEMBERRec.instrumentGroupId))
                .and(CCINSTRUMENTGROUPMEMBERRec.instrumentId.equalTo(CCINSTRUMENTKey.id))
                .and(CCSCHEMERec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCSCHEMERec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCSCHEMERec.code.equalTo(sourceSSRec.getSchemeCode()))
                .and(CCINSTRUMENTRec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCINSTRUMENTRec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCINSTRUMENTRec.isActive.equalTo(YES))
                .orderByAscending(CCINSTRUMENTGROUPMEMBERRec.aix2)
                .setMaxResults(1)

        String[] result = getResult(edoiFacade.scroll(instrumentCodeQuery));
        result
    }

    private Query getLatestSSRecToUpdateQuery(CCSAMPLESCHEMERec latestSSRec) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(latestSSRec.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(latestSSRec.getLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(latestSSRec.getJobCode()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(latestSSRec.getSampleCode()));
        query.and(CCSAMPLESCHEMERec.schemeLaboratoryCode.equalTo(sanitiseCode(latestSSRec.getSchemeLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.schemeCode.equalTo(latestSSRec.getSchemeCode()));
        query.and(CCSAMPLESCHEMERec.schemeVersionNumber.equalTo(latestSSRec.getSchemeVersionNumber()));
        query
    }

    private String sanitiseCode(String value) {
        if (StringUtils.isBlank(value)) {
            return NULL_VALUE;
        } else {
            return value;
        }
    }
}
import com.google.common.collect.Lists
import com.mincom.cclas.domain.job.JobRecKey
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.schemeversion.SchemeVersionKey
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.cclas.util.SystemConstants
import com.mincom.cclas.util.rec.RecKeyUtil
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTKey
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTRec
import com.mincom.ellipse.edoi.ejb.ccinstrumentgroupmember.CCINSTRUMENTGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLEKey
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLERec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.types.m2000.instances.CCSampleDTO
import com.mincom.eql.Query
import com.mincom.eql.QueryFactory
import com.mincom.eql.UpdateQuery
import com.mincom.eql.impl.UpdateQueryImpl
import org.apache.commons.lang3.StringUtils

import static com.mincom.cclas.query.QueryResultsCaster.getResult
import static com.mincom.cclas.type.util.UIDUtil.convertToStringSet
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.base.util.TypeUtil.containsValue
import static java.util.Arrays.asList

public class CCSampleService_multipleAddSchemesFromSamples extends CoreServiceHook {
    //This thread-local based map will be used to keep differentiate new SampleSchemes that are added into Samples.
    private final
    static ThreadLocal<Map<String, Set<CCSAMPLESCHEMERec>>> SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL = new ThreadLocal<>();

    private SampleRepository sampleRepository = getBean(SampleRepository.class);
    private TimedEDOIFacade edoiFacade = getBean(TimedEDOIFacade.class);

    @Override
    Object onPreExecute(Object inputs) {
        //always reset it first
        SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.set(null);

        CCSampleDTO[] sampleDTOS = inputs as CCSampleDTO[];

        if (sampleDTOS == null || sampleDTOS.length == 0) {
            return null;
        }

        Map<String, Set<CCSAMPLESCHEMERec>> destSampleIdAndSSRecMap = buildDestSampleIdAndSSRecMap(sampleDTOS);
        SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.set(destSampleIdAndSSRecMap);

        return null
    }

    private Map<String, Set<CCSAMPLESCHEMERec>> buildDestSampleIdAndSSRecMap(CCSampleDTO[] sampleDTOS) {
        Set<String> destinationSampleIds = new HashSet<>();
        for (CCSampleDTO sampleDTO : sampleDTOS) {
            if (sampleDTO.getSampleIds() != null) {
                destinationSampleIds.addAll(convertToStringSet(sampleDTO.getSampleIds()));
            }
        }

        if (destinationSampleIds.isEmpty()) {
            return null;
        }

        JobRecKey destinationJobKey = findDestinationJobKey(destinationSampleIds)

        return buildSampleIdAndSSRecMap(destinationJobKey, destinationSampleIds);
    }

    private Map<String, Set<CCSAMPLESCHEMERec>> buildSampleIdAndSSRecMap(JobRecKey jobRecKey, Set<String> requiredSampleIds) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode

        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(jobRecKey.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(jobRecKey.getLaboratoryCode()));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(jobRecKey.getCode()));

        Map<String, Set<CCSAMPLESCHEMERec>> map = new HashMap<>();

        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();

            //skip those samples that are not in the destination sampleIds list
            if (!requiredSampleIds.contains(ssRec.getSampleId())) {
                continue;
            }

            Set<CCSAMPLESCHEMERec> ssSet = map.get(ssRec.getSampleId());
            if (ssSet == null) {
                ssSet = new HashSet<>();
                map.put(ssRec.getSampleId(), ssSet);
            }
            ssSet.add(ssRec);
        }

        return map;
    }

    private JobRecKey findDestinationJobKey(Set<String> destinationSampleIds) {
        //since destination SampleIds always come from the same job, so it is safe to assume all destination sampleIds are belong to a same job.
        String firstSampleId = destinationSampleIds.iterator().next();

        List<CCSAMPLERec> sampleRecs = sampleRepository.loadSparseRecs(Lists.asList(firstSampleId),
                CCSAMPLEKey.id,
                CCSAMPLERec.organisationCode,
                CCSAMPLERec.laboratoryCode,
                CCSAMPLERec.jobCode,
                CCSAMPLERec.code);

        if (sampleRecs.isEmpty()) {
            throw new RuntimeException("Unable to locale jobCode for the given sampleId:" + firstSampleId);
        }

        CCSAMPLERec sampleRec = sampleRecs.get(0);
        JobRecKey jobKey = new JobRecKey(sampleRec.getOrganisationCode(), sanitiseCode(sampleRec.getLaboratoryCode()), sampleRec.getJobCode())
        jobKey
    }

    private Map<JobRecKey, Set<String>> buildJobRecKeyAndSampleIdsMap(CCSampleDTO[] sampleDTOS) {
        Map<JobRecKey, Set<String>> map = new HashMap<>();

        for (CCSampleDTO sampleDTO : sampleDTOS) {
            JobRecKey jobKey = new JobRecKey(sampleDTO.getOrganisationCode().getValue(),
                    sanitiseCode(sampleDTO.getLaboratoryCode().getValue()), sampleDTO.getJobCode().value);

            Set<String> sampleIds = map.get(jobKey);
            if (sampleIds == null) {
                sampleIds = new HashSet<>();
                map.put(jobKey, sampleIds);
            }
            sampleIds.add(sampleDTO.getSampleId().getValue());
        }

        return map;
    }

    private Map<SchemeVersionKey, CCSAMPLESCHEMERec> buildSourceSchemeVersionKeyAndSSRecMap(CCSampleDTO[] sampleDTOS) {
        Map<JobRecKey, Set<String>> sourceJobRecKeyAndSampleIdsMap = buildJobRecKeyAndSampleIdsMap(sampleDTOS);

        Map<String, Set<CCSAMPLESCHEMERec>> sourceSampleIdAndSSMap = new HashMap<>();

        for (Map.Entry<JobRecKey, Set<String>> entry : sourceJobRecKeyAndSampleIdsMap.entrySet()) {
            JobRecKey sourceJobKey = entry.getKey();
            Set<String> sourceSampleIds = entry.getValue();
            sourceSampleIdAndSSMap.putAll(buildSampleIdAndSSRecMap(sourceJobKey, sourceSampleIds));
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap = new HashMap<>();
        //if multiple sources of samples have the same scheme and schemeVersion, the first sampleDTO will take precedence.
        for (CCSampleDTO sampleDTO : sampleDTOS) {
            Set<CCSAMPLESCHEMERec> sourceSSMap = sourceSampleIdAndSSMap.get(sampleDTO.getSampleId().getValue());

            if (sourceSSMap == null) {
                continue;
            }

            for (CCSAMPLESCHEMERec sourceSSRec : sourceSSMap) {
                SchemeVersionKey schemeVersionKey = toSchemeVersionKey(sourceSSRec)

                sourceSchemeVersionKeyAndSSRecMap.putIfAbsent(schemeVersionKey, sourceSSRec);
            }
        }

        return sourceSchemeVersionKeyAndSSRecMap;
    }

    private SchemeVersionKey toSchemeVersionKey(CCSAMPLESCHEMERec sourceSSRec) {
        SchemeVersionKey schemeVersionKey = new SchemeVersionKey(
                sourceSSRec.getOrganisationCode(),
                sanitiseCode(sourceSSRec.getSchemeLaboratoryCode()),
                sourceSSRec.getSchemeCode(),
                sourceSSRec.getSchemeVersionNumber());
        schemeVersionKey
    }


    @Override
    Object onPostExecute(Object inputs, Object result, Object returnWarnings) {

        Map<String, Set<CCSAMPLESCHEMERec>> destSampleIdAndOriginalSSRecMap = SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.get();
        //skip it if the map is not set
        if (destSampleIdAndOriginalSSRecMap == null) {
            return result;
        }

        CCSampleDTO[] sampleDTOS = inputs as CCSampleDTO[];

        if (!containsValue(sampleDTOS[0].getLaboratoryCode())) {
            return result
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap =
                buildSourceSchemeVersionKeyAndSSRecMap(sampleDTOS);


        Map<String, Set<CCSAMPLESCHEMERec>> destSampleIdAndLatestSSRecMap = buildDestSampleIdAndSSRecMap(sampleDTOS);

        List<UpdateQuery> ssUpdateQueries = new ArrayList<>();

        for (Map.Entry<String, Set<CCSAMPLESCHEMERec>> entry : destSampleIdAndLatestSSRecMap) {
            String destSampleId = entry.getKey();
            Set<CCSAMPLESCHEMERec> destLatestSSRecSet = entry.getValue();

            Set<CCSAMPLESCHEMERec> destOriginalSSRecSet = destSampleIdAndOriginalSSRecMap.get(destSampleId);

            for (CCSAMPLESCHEMERec latestSSRec : destLatestSSRecSet) {
                //check whether it is a newly added SS, if so, we need to update the instrumentCode
                if (destOriginalSSRecSet == null || !destOriginalSSRecSet.contains(latestSSRec)) {

                    Query query = getLatestSSRecQuery(latestSSRec)

                    CCSAMPLESCHEMERec sourceSSRec = sourceSchemeVersionKeyAndSSRecMap.get(toSchemeVersionKey(latestSSRec));
                    if (sourceSSRec == null) {
                        throw new RuntimeException("Unable to locate source SSRec based on schemeVersionKey. This must be dev's bug.");
                    }

                    String instrumentCode = null;
                    String instrumentLabCode = null;
                    if (!containsValue(sourceSSRec.getInstrumentCode())) {
                        String [] results = getInstrumentRecViaInstrumentGroup(sourceSSRec)

                        if (results != null && results.length == 2) {
                            instrumentCode = results[0]
                            instrumentLabCode = results[1]
                        }
                    }

                    UpdateQuery updateQuery = new UpdateQueryImpl(query);

                    updateQuery.set(CCSAMPLESCHEMERec.instrumentLabCode,
                            sanitiseCode(containsValue(sourceSSRec.getInstrumentLabCode()) ? sourceSSRec.getInstrumentLabCode() : instrumentLabCode))
                    updateQuery.set(CCSAMPLESCHEMERec.instrumentCode,
                            sanitiseCode(containsValue(sourceSSRec.getInstrumentCode()) ? sourceSSRec.getInstrumentCode() : instrumentCode))

                    ssUpdateQueries.add(updateQuery);
                }
            }
        }

        if (!ssUpdateQueries.isEmpty()) {
            edoiFacade.updateAllBatch(ssUpdateQueries);
        }

        SAMPLE_ID_AND_ORIGINAL_SS_MAP_THREAD_LOCAL.set(null);
        return result
    }

    private Query getLatestSSRecQuery(CCSAMPLESCHEMERec latestSSRec) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(latestSSRec.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(latestSSRec.getLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(latestSSRec.getJobCode()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(latestSSRec.getSampleCode()));
        query.and(CCSAMPLESCHEMERec.schemeLaboratoryCode.equalTo(sanitiseCode(latestSSRec.getSchemeLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.schemeCode.equalTo(latestSSRec.getSchemeCode()));
        query.and(CCSAMPLESCHEMERec.schemeVersionNumber.equalTo(latestSSRec.getSchemeVersionNumber()));
        query
    }


    private String[] getInstrumentRecViaInstrumentGroup(CCSAMPLESCHEMERec sourceSSRec) {
        Query instrumentCodeQuery = QueryFactory.query(CCINSTRUMENTRec.class);
        instrumentCodeQuery
                .columns(asList(CCINSTRUMENTRec.code, CCINSTRUMENTRec.laboratoryCode))
                .and(CCSCHEMERec.instrumentGroupId.equalTo(CCINSTRUMENTGROUPMEMBERRec.instrumentGroupId))
                .and(CCINSTRUMENTGROUPMEMBERRec.instrumentId.equalTo(CCINSTRUMENTKey.id))
                .and(CCSCHEMERec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCSCHEMERec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCSCHEMERec.code.equalTo(sourceSSRec.getSchemeCode()))
                .and(CCINSTRUMENTRec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCINSTRUMENTRec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCINSTRUMENTRec.isActive.equalTo(YES))
                .orderByAscending(CCINSTRUMENTGROUPMEMBERRec.aix2)
                .setMaxResults(1)

        String[] result = getResult(edoiFacade.scroll(instrumentCodeQuery));
        result
    }

    private String sanitiseCode(String value){
        if(StringUtils.isBlank(value)){
            return NULL_VALUE;
        }else{
            return value;
        }
    }
}
import com.mincom.cclas.domain.job.JobRecKey
import com.mincom.cclas.domain.job.JobRepository
import com.mincom.cclas.domain.job.scheme.JobSchemeRepository
import com.mincom.cclas.domain.reporttemplate.ReportTemplateReportTypeEnum
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.sample.scheme.SampleSchemeRepository
import com.mincom.cclas.impl.domain.MessageUtil
import com.mincom.cclas.impl.domain.reportrequest.builder.generator.ReportGenerator
import com.mincom.cclas.query.FastUpdateQuery
import com.mincom.cclas.query.NormalizedConstraintBuilder
import com.mincom.cclas.query.QueryUtil
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.cclas.report.request.ReportRequestBuilder
import com.mincom.cclas.report.request.ReportRequestOutput
import com.mincom.cclas.report.request.ReportRequestService
import com.mincom.cclas.report.request.ReportableRule
import com.mincom.cclas.security.SecurityContextService
import com.mincom.ellipse.app.security.SecurityToken
import com.mincom.ellipse.edoi.ejb.QueryResults
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.cccategory.CCCATEGORYKey
import com.mincom.ellipse.edoi.ejb.cccategory.CCCATEGORYRec
import com.mincom.ellipse.edoi.ejb.ccdevice.CCDEVICEKey
import com.mincom.ellipse.edoi.ejb.ccdevice.CCDEVICERec
import com.mincom.ellipse.edoi.ejb.ccjobscheme.CCJOBSCHEMERec
import com.mincom.ellipse.edoi.ejb.ccqctype.CCQCTYPEKey
import com.mincom.ellipse.edoi.ejb.ccqctype.CCQCTYPERec
import com.mincom.ellipse.edoi.ejb.ccreporttemplate.CCREPORTTEMPLATEKey
import com.mincom.ellipse.edoi.ejb.ccreporttemplate.CCREPORTTEMPLATERec
import com.mincom.ellipse.edoi.ejb.ccrepreqsample.CCREPREQSAMPLERec
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLEKey
import com.mincom.ellipse.edoi.ejb.ccsample.CCSAMPLERec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.edoi.ejb.ccschemegroup.CCSCHEMEGROUPKey
import com.mincom.ellipse.edoi.ejb.ccschemegroup.CCSCHEMEGROUPRec
import com.mincom.ellipse.edoi.ejb.ccschemegroupmember.CCSCHEMEGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.msf0p5.MSF0P5Rec
import com.mincom.ellipse.errors.Error
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.security.SecurityTokenServiceLocator
import com.mincom.ellipse.service.ServiceResult
import com.mincom.ellipse.service.m2000.ccjob.CCJobService
import com.mincom.ellipse.service.m2000.ccjobpaperwork.CCJobPaperworkService
import com.mincom.ellipse.service.m2000.ccreportrequest.CCReportRequestService
import com.mincom.ellipse.types.m2000.instances.*
import com.mincom.eql.Constraint
import com.mincom.eql.Query
import com.mincom.eql.StringConstraint
import com.mincom.eql.UpdateQuery
import com.mincom.eql.common.Rec
import com.mincom.eql.impl.QueryImpl
import com.mincom.eql.impl.StringConstraintImpl
import com.mincom.eql.impl.UpdateQueryImpl
import com.mincom.ji.cqrs.Messages
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static CCJobBatchService_multipleCreateValidations.canProcess
import static CCJobBatchService_multipleCreateValidations.validateHasJob
import static com.google.common.collect.Lists.newArrayList
import static com.google.common.collect.Sets.newHashSet
import static com.mincom.cclas.query.ConstraintUtil.equalAny
import static com.mincom.cclas.query.QueryResultsCaster.getResults
import static com.mincom.cclas.query.QueryUtil.alias
import static com.mincom.cclas.query.QueryUtil.andEqual
import static com.mincom.cclas.service.JIServiceUtil.locateJIService
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.m2000.types.AnalyticalTypeType.REPLICATE
import static com.mincom.ellipse.types.m2000.types.AnalyticalTypeType.DUPLICATE
import static com.mincom.ellipse.types.m2000.types.AnalyticalTypeType.UNKNOWN
import static com.mincom.eql.QueryFactory.query
import static java.util.Arrays.asList
import static org.apache.commons.lang.StringUtils.isNotBlank

/**
 * B-148436 (https://www11.v1host.com/VentyxProd/story.mvc/Summary?oidToken=Story%3A5985069)
 * Determine highest priority from samples in the lab batch, and set the batch priority to match this priority.
 * This is done so users can sort search results by priority, and since could be batching samples from any job,
 * the priority at the job level may not reflect the urgency from the sample level.
 *
 * B-138526 (https://www11.v1host.com/VentyxProd/story.mvc/Summary?oidToken=Story%3A5654415)
 * Determine the schemes in the batch and add as a comma separated list of schemes to the description of the batch job,
 * so users can see the schemes in the batch easily from workbook search.
 */
class CCJobBatchService_multipleCreate extends CoreServiceHook {

    protected final static String COMMA_DELIMITER = ",";
    protected final static String INST_BRACKET_START = "["
    protected final static String INST_BRACKET_END = "]"
    protected final static String SCH_BRACKET_START = "("
    protected final static String SCH_BRACKET_END = ")"
    protected static final String ALIAS_UNKNOWN_SAMPLE = "s";
    protected static final String ALIAS_QC_SAMPLE = "qc";

    private CCJobService srvJob
    private JobRepository repoJob
    private SampleRepository repoSample
    private JobSchemeRepository repoJobScheme
    private CCJobPaperworkService srvJobPW;
    private SecurityToken token
    private TimedEDOIFacade tEDOI

    private List<Boolean> autoCreatePaperwork

    private SampleSchemeRepository sampleSchemeRepository = getBean(SampleSchemeRepository.class);
    private JobRepository jobRepository = getBean(JobRepository.class);
    private SecurityContextService securityContextService = getBean(SecurityContextService.class);
    private ReportRequestService reportRequestServiceForRRBuilder = getBean(ReportRequestService.class);
    private CCReportRequestService reportRequestService = getBean(CCReportRequestService.class);
    private ReportGenerator reportGenerator = getBean(ReportGenerator.class);

    private Logger LOGGER = LoggerFactory.getLogger(CCJobBatchService_multipleCreate.class)

    protected CCJobBatchService_multipleCreate() {
        autoCreatePaperwork = []
    }

    protected CCJobBatchService_multipleCreate(JobRepository repoJob, JobSchemeRepository repoJobScheme, CCJobService srvJob, SecurityToken token, TimedEDOIFacade tEDOI, CCJobPaperworkService srvJobPW) {
        // Constructor used for test coverage
        this.srvJob = srvJob
        this.repoJob = repoJob;
        this.repoJobScheme = repoJobScheme;
        this.token = token
        this.tEDOI = tEDOI
        this.srvJobPW = srvJobPW

        autoCreatePaperwork = []
    }

    @Override
    Object onPreExecute(Object dto, Object returnWarnings) {
        // Disable automatic paperwork creation so we can generate the paperwork with the description created in the postExecute hook
        CCJobBatchSessionDTO[] dtoCast = (CCJobBatchSessionDTO[]) dto;
        dtoCast.eachWithIndex { CCJobBatchSessionDTO item, int index ->
            CCJobBatchCreateBatchDTO createBatchDTO = item.getJobBatchCreateBatchDTO()
            autoCreatePaperwork.add(index, createBatchDTO.getAutoCreatePaperwork().getValue())
            createBatchDTO.getAutoCreatePaperwork().setValue(false);
        }
        return null
    }

    @Override
    Object onPostExecute(Object input, Object result, Object returnWarnings) {
        if (!canProcess(result)) {
            return result
        }

        CCJobBatchSessionServiceResult[] resultsCast = (CCJobBatchSessionServiceResult[]) result;

        resultsCast.eachWithIndex { CCJobBatchSessionServiceResult entry, int i ->
            processReplicateResult(resultsCast[i], (Boolean) returnWarnings)
            processOneResult(resultsCast[i], (Boolean) returnWarnings, autoCreatePaperwork.get(i));
        }

        return result
    }

    private List<Object[]> findUnknownsWithPotentialClientQC(OrganisationCode orgCode, LaboratoryCode labCode, JobCode jobCode) {
        Query qcTypeQuery = getQcTypeQuery()

        Query unkSampleQuery = new QueryImpl(CCSAMPLERec.class, ALIAS_QC_SAMPLE)
                .rightJoin(CCSAMPLERec.dupOriginalSampleFK, ALIAS_UNKNOWN_SAMPLE)
                .and(alias(CCSAMPLERec.organisationCode, ALIAS_UNKNOWN_SAMPLE).equalTo(orgCode.getValue()))
                .and(alias(CCSAMPLERec.laboratoryCode, ALIAS_UNKNOWN_SAMPLE).equalTo(labCode.getValue()))
                .and(alias(CCSAMPLERec.jobCode, ALIAS_UNKNOWN_SAMPLE).equalTo(jobCode.getValue()))
                .and(alias(CCSAMPLERec.primaryAnalyticalType, ALIAS_UNKNOWN_SAMPLE).equalTo(UNKNOWN))
                .and(alias(CCSAMPLERec.secondaryQcTypeId, ALIAS_UNKNOWN_SAMPLE).in(qcTypeQuery, CCQCTYPEKey.id)
                .or(alias(CCSAMPLERec.primaryAnalyticalType, ALIAS_QC_SAMPLE).equalTo(REPLICATE)))
                .noAutoI18n()
                .asEntity()

        return getResults(getTimedEDOIFacade().search(unkSampleQuery));
    }


    private List<CCSAMPLERec> queryPotentialUnknowns(OrganisationCode orgCode, LaboratoryCode labCode, JobCode jobCode,
                                                     Set<String> nonPotentialUnknowns, int maxSize) {

        Query unkSampleQuery = new QueryImpl(CCSAMPLERec.class)
                .and(CCSAMPLERec.organisationCode.equalTo(orgCode.getValue()))
                .and(CCSAMPLERec.laboratoryCode.equalTo(labCode.getValue()))
                .and(CCSAMPLERec.jobCode.equalTo(jobCode.getValue()))
                .and(CCSAMPLERec.primaryAnalyticalType.equalTo(UNKNOWN))
                .and(CCSAMPLERec.code.notIn(nonPotentialUnknowns))
                .noAutoI18n()
                .setMaxResults(maxSize)
                .asEntity()

        return getResults(getTimedEDOIFacade().search(unkSampleQuery));
    }

    private static Query getQcTypeQuery() {
        Query qcTypeQuery = query(CCQCTYPERec.class)
                .and(CCQCTYPERec.qcTypeCategoryId.equalTo(CCCATEGORYKey.id))
                .and(CCCATEGORYRec.code.equalTo('SECONDARY'))
        qcTypeQuery
    }


    CCJobBatchSessionServiceResult processReplicateResult(CCJobBatchSessionServiceResult result, Boolean returnWarnings) {
        if (!canProcess(result)) {
            return result;
        }
        Messages messages = new Messages();
        MessageUtil.copyMessages(messages, result);

        CCJobBatchSessionDTO jobBatchDTO = result.getCCJobBatchSessionDTO();
        List<Object[]> recs = findUnknownsWithPotentialClientQC(jobBatchDTO.getOrganisationCode(), jobBatchDTO.getLaboratoryCode(),
                jobBatchDTO.getJobCode());

        Set<String> nonPotentialUnknowns = new HashSet<>();
        LinkedHashMap<SampleRecKey, Set<CCSAMPLERec>> unknownWithClientQcMap = getUnknownWithClientQC(recs, nonPotentialUnknowns)

        List<CCSAMPLERec> pontentialNewUnknowns = queryPotentialUnknowns(jobBatchDTO.getOrganisationCode(), jobBatchDTO.getLaboratoryCode(),
                jobBatchDTO.getJobCode(), nonPotentialUnknowns, unknownWithClientQcMap.size());

        FastUpdateQuery updateQuery = getFastUpdateQueryToUpdateSample()

        for (int i = 0; i < pontentialNewUnknowns.size(); i++) {
            CCSAMPLERec newPontentialUnknown = pontentialNewUnknowns.get(i);
            Set<CCSAMPLERec> movableRepInUnknown = (new ArrayList<Set<CCSAMPLERec>>(unknownWithClientQcMap.values())).get(i)// -- i unknown
            for (CCSAMPLERec movableRepRec : movableRepInUnknown) {
                Object[] updateValues = [newPontentialUnknown.getPrimaryKey().getId(),
                                         newPontentialUnknown.getName(),
                                         newPontentialUnknown.getDescription(),
                                         newPontentialUnknown.getClientSampleName()]

                Object[] paramValues = [movableRepRec.getOrganisationCode(),
                                        movableRepRec.getLaboratoryCode(),
                                        movableRepRec.getJobCode(),
                                        movableRepRec.getCode()]

                updateQuery.addRow(updateValues, paramValues);
            }
        }
        getTimedEDOIFacade().executeUpdate(updateQuery);

        return result;
    }

    private LinkedHashMap<SampleRecKey, Set<CCSAMPLERec>> getUnknownWithClientQC(List<Object[]> recs, HashSet<String> nonPotentialUnknowns) {
        LinkedHashMap<SampleRecKey, Set<CCSAMPLERec>> unknownWithClientQcMap = new LinkedHashMap<>();

        for (Object[] rec : recs) {
            CCSAMPLERec[] values = (CCSAMPLERec[]) rec;
            CCSAMPLERec unknown = values[1];
            nonPotentialUnknowns.add(unknown.getCode())

            if (values[0] != null) {
                CCSAMPLERec qc = values[0];

                if (isClientQC(unknown, qc)) {
                    SampleRecKey sampleKey = toSampleKey(unknown.getOrganisationCode(),
                            unknown.getLaboratoryCode(),
                            unknown.getJobCode(),
                            unknown.getCode(),
                            unknown.getPrimaryKey().getId())
                    Set<CCSAMPLERec> sSet = unknownWithClientQcMap.get(sampleKey);

                    if (sSet == null) {
                        sSet = new HashSet<>();
                        unknownWithClientQcMap.put(sampleKey, sSet);
                    }

                    sSet.add(qc);
                }
            }
        }
        return unknownWithClientQcMap;
    }

    private FastUpdateQuery getFastUpdateQueryToUpdateSample() {
        return new FastUpdateQuery(
                CCSAMPLERec.class,
                newArrayList(CCSAMPLERec.dupOriginalSampleId,
                        new StringConstraintImpl(CCSAMPLERec.class, "name", 40, true, false).columnName("NAME") as StringConstraint,
                        CCSAMPLERec.description,
                        CCSAMPLERec.clientSampleName
                ),
                newArrayList(
                        CCSAMPLERec.organisationCode,
                        CCSAMPLERec.laboratoryCode,
                        CCSAMPLERec.jobCode,
                        CCSAMPLERec.code
                )
        );
    }

    private boolean isClientQC(CCSAMPLERec unknown, CCSAMPLERec qc) {
        unknown.getSecondaryQcTypeId() != null && qc.getPrimaryAnalyticalType() == REPLICATE
    }

    private SampleRecKey toSampleKey(String orgCode, String labCode, String jobCode, String sampleCode, String sampleId) {
        SampleRecKey sampleKey = new SampleRecKey(orgCode, labCode, jobCode, sampleCode, sampleId);
        sampleKey
    }

    CCJobBatchSessionServiceResult processOneResult(CCJobBatchSessionServiceResult result, Boolean returnWarnings, Boolean paperwork) {
        if (!canProcess(result)) {
            return result;
        }

        Messages messages = new Messages();
        MessageUtil.copyMessages(messages, result);

        // TODO: Is it important to return Ii8N?
        CCJobBatchSessionDTO jobBatchDTO = result.getCCJobBatchSessionDTO();
        CCJobDTO jobDTO = getJobRepository().findByCode(jobBatchDTO.getJobCode(), jobBatchDTO.getOrganisationCode(), jobBatchDTO.getLaboratoryCode(), messages);
        if (messages.hasAnyErrors()) {
            MessageUtil.copyMessages(result, messages)
            return result
        }

        validateHasJob(jobDTO, jobBatchDTO, result)
        if (result.hasErrors()) {
            return result
        }

        updatePriorityDescriptionAndGeneratePaperwork(jobDTO, jobBatchDTO, messages, result, paperwork, returnWarnings)
        if (paperwork){
            // Change made as per B-157386; all paperwork should now honour batch dialog checkbox
            createSchemeGroupBasedPaperworkOrLabels(jobDTO, messages)
        }
        return result;
    }

    private Map<String, Set<String>> findSchemeGroupAndSchemesMap(Set<CCJOBSCHEMERec> jobSchemes) {
        Query query = query(CCSCHEMERec.class);

        query.and(CCSCHEMEKey.id.equalTo(CCSCHEMEGROUPMEMBERRec.schemeId));
        query.and(CCSCHEMEGROUPMEMBERRec.schemeGroupId.equalTo(CCSCHEMEGROUPKey.id));

        NormalizedConstraintBuilder schemeConstraintBuilder = new NormalizedConstraintBuilder();
        for (CCJOBSCHEMERec jobScheme : jobSchemes) {
            schemeConstraintBuilder.newEntry();

            schemeConstraintBuilder.setEqual(CCSCHEMERec.organisationCode, jobScheme.organisationCode);
            schemeConstraintBuilder.setEqual(CCSCHEMERec.laboratoryCode, jobScheme.schemeLaboratoryCode);
            schemeConstraintBuilder.setEqual(CCSCHEMERec.code, jobScheme.schemeCode);
        }

        query.and(schemeConstraintBuilder.buildConstraints());

        query.columns(newArrayList(CCSCHEMEGROUPRec.code, CCSCHEMERec.code));

        ScrollableResults scrollableResults = getTimedEDOIFacade().scroll(query);
        Map<String, Set<String>> schemeGroupAndSchemesMap = new HashMap<>();
        while (scrollableResults.hasNext()) {
            Object[] row = scrollableResults.next();

            String schemeGroupCode = row[0];
            String schemeCode = row[1];

            Set<String> schemeCodes = schemeGroupAndSchemesMap.get(schemeGroupCode);
            if (schemeCodes == null) {
                schemeCodes = new HashSet<>();
                schemeGroupAndSchemesMap.put(schemeGroupCode, schemeCodes);
            }
            schemeCodes.add(schemeCode);
        }

        schemeGroupAndSchemesMap
    }

    private Map<String, Set<CCREPORTTEMPLATERec>> findSchemeGroupCodeAndReportTemplatesMap(Set<String> schemeGroupCodes) {
        Map<String, Set<CCREPORTTEMPLATERec>> schemeGroupAndReportTemplatesMap = new HashMap<>();
        if (schemeGroupCodes.isEmpty()) {
            return schemeGroupAndReportTemplatesMap;
        }

        Query query = query(CCREPORTTEMPLATERec.class)
        query.join(CCREPORTTEMPLATEKey.id, MSF0P5Rec.entityKey);
        query.and(MSF0P5Rec.entityType.equalTo("CCReportTemplateService.CCReportTemplate.ReportSchemeGroup"));
        query.and(equalAny(MSF0P5Rec.propertyValue, schemeGroupCodes));
        query.and(CCREPORTTEMPLATERec.organisationCode.equalTo(securityContextService.contextOrganisationCode()));
        query.and(CCREPORTTEMPLATERec.laboratoryCode.in(NULL_VALUE, securityContextService.contextLaboratoryCode()));
        query.and(CCREPORTTEMPLATERec.isActive.equalTo(YES));
        query.and(CCREPORTTEMPLATERec.reportType.in(ReportTemplateReportTypeEnum.PAPERWORK.value(), ReportTemplateReportTypeEnum.LABELS.value()));

        query.columns(newArrayList(
                MSF0P5Rec.propertyValue,//0
                CCREPORTTEMPLATEKey.id, //1
                CCREPORTTEMPLATERec.code,  //2
                CCREPORTTEMPLATERec.outputFileNameSyntaxId,//3
                CCREPORTTEMPLATERec.defaultOutputFormat, //4
                CCREPORTTEMPLATERec.deviceId //5
        ));

        ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            Object[] rows = scrollableResults.next();

            String schemeGroupCode = rows[0];

            CCREPORTTEMPLATERec reportTemplateRec = new CCREPORTTEMPLATERec();
            reportTemplateRec.setPrimaryKey(new CCREPORTTEMPLATEKey((String) rows[1]));
            reportTemplateRec.setCode((String) rows[2])
            reportTemplateRec.setOutputFileNameSyntaxId((String) rows[3])
            reportTemplateRec.setDefaultOutputFormat((String) rows[4])
            reportTemplateRec.setDeviceId((String) rows[5])

            Set<CCREPORTTEMPLATERec> rrRecs = schemeGroupAndReportTemplatesMap.get(schemeGroupCode);
            if (rrRecs == null) {
                rrRecs = new HashSet<>();
                schemeGroupAndReportTemplatesMap.put(schemeGroupCode, rrRecs);
            }

            rrRecs.add(reportTemplateRec);
        }

        schemeGroupAndReportTemplatesMap;
    }

    private Map<String, Set<CCSAMPLESCHEMERec>> findSchemeAndSampleSchemesMap(CCJobDTO jobDTO) {
        Query query = query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(jobDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(jobDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(jobDTO.getCode().getValue()));

        query.columns(newArrayList(
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeVersionNumber
        ))

        query.asEntity();

        ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
        Map<String, Set<CCSAMPLESCHEMERec>> schemeAndSampleSchemesMap = new HashMap<>();
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = scrollableResults.next() as CCSAMPLESCHEMERec;

            Set<CCSAMPLESCHEMERec> ssSet = schemeAndSampleSchemesMap.get(ssRec.getSchemeCode());
            if (ssSet == null) {
                ssSet = new HashSet<>();
                schemeAndSampleSchemesMap.put(ssRec.getSchemeCode(), ssSet);
            }
            ssSet.add(ssRec);
        }
        schemeAndSampleSchemesMap
    }

    private Map<String, String> findDeviceIdAndCodeMap(Collection<Set<CCREPORTTEMPLATERec>> reportTemplateSetList) {
        Set<String> deviceIds = new HashSet<>();

        for (Set<CCREPORTTEMPLATERec> reportTemplateSet : reportTemplateSetList) {
            for (CCREPORTTEMPLATERec rr : reportTemplateSet) {
                if (isNotBlank(rr.deviceId)) {
                    deviceIds.add(rr.deviceId);
                }
            }
        }

        Map<String, String> deviceIdAndCodeMap = new HashMap<>();
        if (!deviceIds.isEmpty()) {
            Query query = query(CCDEVICERec.class);
            query.and(equalAny(CCDEVICEKey.id, deviceIds));
            query.columns(newArrayList(CCDEVICEKey.id, CCDEVICERec.code))

            ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
            while (scrollableResults.hasNext()) {
                Object[] row = scrollableResults.next();

                deviceIdAndCodeMap.put((String) row[0], (String) row[1]);
            }
        }

        deviceIdAndCodeMap;
    }

    private void createSchemeGroupBasedPaperworkOrLabels(CCJobDTO jobDTO, Messages messages) {
        JobRecKey jobRecKey = new JobRecKey(jobDTO.getOrganisationCode().getValue(), jobDTO.getLaboratoryCode().getValue(),
                jobDTO.getCode().getValue());

        Set<CCJOBSCHEMERec> schemes = jobRepository.findJobSchemes(newHashSet(jobRecKey));
        if (schemes.isEmpty()) {
            return;
        }

        Map<String, Set<String>> schemeGroupCodeAndSchemeCodesMap = findSchemeGroupAndSchemesMap(schemes);
        Map<String, Set<CCREPORTTEMPLATERec>> schemeGroupCodeAndReportTemplatesMap = findSchemeGroupCodeAndReportTemplatesMap(schemeGroupCodeAndSchemeCodesMap.keySet());
        Map<String, Set<CCSAMPLESCHEMERec>> schemeAndSampleSchemesMap = findSchemeAndSampleSchemesMap(jobDTO);

        Map<String, String> deviceIdAndCodeMap = findDeviceIdAndCodeMap(schemeGroupCodeAndReportTemplatesMap.values());

        Map<String, CCSAMPLERec> sampleMap = findSampleMap(jobDTO);

        List<CCReportRequestDTO> reportRequestDTOList = new ArrayList<>();
        List<ReportRequestOutput> reportRequestOutputList = new ArrayList<>();

        for (Map.Entry<String, Set<CCREPORTTEMPLATERec>> entry : schemeGroupCodeAndReportTemplatesMap.entrySet()) {
            String schemeGroupCode = entry.getKey();
            Set<CCREPORTTEMPLATERec> reportTemplates = entry.getValue();

            Set<String> schemeCodes = schemeGroupCodeAndSchemeCodesMap.get(schemeGroupCode);
            if (schemeCodes != null) {
                for (String schemeCode : schemeCodes) {
                    Set<CCSAMPLESCHEMERec> ssRecs = schemeAndSampleSchemesMap.get(schemeCode);

                    for (CCREPORTTEMPLATERec reportTemplate : reportTemplates) {
                        ReportRequestBuilder rrBuilder = reportRequestServiceForRRBuilder.createBuilder(reportTemplate.getCode())
                        rrBuilder.setRequiredNewTransaction(false);
                        rrBuilder.setReportRequestType(new ReportRequestReportType(reportTemplate.getReportType()));

                        rrBuilder.setReportRequestCodeSyntaxCode("REP_REQ_CODE_SYNTAX")
                        rrBuilder.setReportRequestNameSyntaxCode("REP_REQ_NAME_SYNTAX")

                        String deviceCode = deviceIdAndCodeMap.get(reportTemplate.getDeviceId());
                        if (isNotBlank(deviceCode)) {
                            rrBuilder.setNumberPrinterCopies(1)
                            rrBuilder.setPrinterCode(deviceCode);
                        }

                        rrBuilder.addJob(jobDTO.getCode().getValue());

                        for (CCSAMPLESCHEMERec ss : ssRecs) {
                            rrBuilder.addSample(jobDTO.getCode().getValue(), ss.getSampleCode(), ReportableRule.None);
                        }
                        rrBuilder.addSchemesForJobWithName(jobDTO.getCode().getValue(), ReportableRule.None, schemeCode);

                        ReportRequestOutput reportRequestOutput = rrBuilder.build(null);
                        CCReportRequestDTO reportRequestDTO = reportRequestOutput.getReportRequest()

                        reportRequestDTOList.add(reportRequestDTO);
                        updateReportRequestSampleReportableFlags(reportRequestDTO, ssRecs, sampleMap);

                        reportRequestOutputList.add(reportRequestOutput);
                    }
                }
            }
        }

        updateReportRequestName(reportRequestDTOList, jobDTO);
        generateReport(reportRequestOutputList);
    }

    private void generateReport(List<ReportRequestOutput> reportRequestOutputList) {
        for (ReportRequestOutput reportRequestOutput : reportRequestOutputList) {
            reportGenerator.generateReport(null, reportRequestOutput, false);
        }
    }

    private Map<String, CCSAMPLERec> findSampleMap(CCJobDTO jobDTO) {
        Query query = query(CCSAMPLERec.class);

        query.and(CCSAMPLERec.organisationCode.equalTo(jobDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLERec.laboratoryCode.equalTo(jobDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLERec.jobCode.equalTo(jobDTO.getCode().getValue()));

        query.columns(newArrayList(
                CCSAMPLEKey.id,
                CCSAMPLERec.organisationCode,
                CCSAMPLERec.laboratoryCode,
                CCSAMPLERec.jobCode,
                CCSAMPLERec.code,
                CCSAMPLERec.isGenerateLabel,
                CCSAMPLERec.isGeneratePaperwork
        ))
        query.asEntity();

        ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
        Map<String, CCSAMPLERec> sampleMap = new HashMap();
        while (scrollableResults.hasNext()) {
            CCSAMPLERec sampleRec = (CCSAMPLERec) scrollableResults.next();
            sampleMap.put(sampleRec.getCode(), sampleRec);
        }
        sampleMap;
    }

    private void updateReportRequestSampleReportableFlags(CCReportRequestDTO reportRequestDTO,
                                                          Set<CCSAMPLESCHEMERec> ssRecs,
                                                          Map<String, CCSAMPLERec> sampleMap) {
        if (ssRecs.isEmpty()) {
            return;
        }

        List<UpdateQuery> updateQueries = new ArrayList<>();

        for (CCSAMPLESCHEMERec ssRec : ssRecs) {
            Query rrSampleQuery = query(CCREPREQSAMPLERec.class);
            rrSampleQuery.and(CCREPREQSAMPLERec.organisationCode.equalTo(ssRec.getOrganisationCode()));
            rrSampleQuery.and(CCREPREQSAMPLERec.laboratoryCode.equalTo(ssRec.getLaboratoryCode()));
            rrSampleQuery.and(CCREPREQSAMPLERec.repReqCode.equalTo(reportRequestDTO.getCode().getValue()));

            rrSampleQuery.and(CCREPREQSAMPLERec.jobCode.equalTo(ssRec.getJobCode()));
            rrSampleQuery.and(CCREPREQSAMPLERec.sampleCode.equalTo(ssRec.getSampleCode()));

            UpdateQuery updateQuery = new UpdateQueryImpl(rrSampleQuery);

            CCSAMPLERec sample = sampleMap.get(ssRec.getSampleCode());

            if (ReportTemplateReportTypeEnum.LABELS.value().equals(reportRequestDTO.getReportType().getValue())) {
                updateQuery.set(CCREPREQSAMPLERec.isReportable, sample.getIsGenerateLabel());
            } else if (ReportTemplateReportTypeEnum.PAPERWORK.value().equals(reportRequestDTO.getReportType().getValue())) {
                updateQuery.set(CCREPREQSAMPLERec.isReportable, sample.getIsGeneratePaperwork());
            } else {
                throw new RuntimeException("Un-expected reportType - " + reportRequestDTO.getReportType().getValue());
            }
            updateQueries.add(updateQuery);
        }

        timedEDOIFacade.updateAllBatch(updateQueries);
    }

    private void updateReportRequestName(List<CCReportRequestDTO> reportRequestDTOList, CCJobDTO jobDTO) {
        if (reportRequestDTOList.isEmpty()) {
            return;
        }

        //change the generated report request name to be the same as batch job name.
        int index = 1;
        for (CCReportRequestDTO reportRequestDTO : reportRequestDTOList) {
            reportRequestDTO.getName().setValue(jobDTO.getName().getValue() + "_" + index);
            index++;
        }

        CCReportRequestServiceResult[] results = reportRequestService.multipleUpdate(securityContextService.securityToken(),
                reportRequestDTOList.toArray(new CCReportRequestDTO[reportRequestDTOList.size()]), false);

        throwExceptionIfAnyError(results);
    }

    private static <T extends ServiceResult> void throwExceptionIfAnyError(T[] results) {
        for (ServiceResult result : results) {
            if (result.hasErrors()) {
                Error[] errors = result.getErrors()

                StringBuilder builder = new StringBuilder();
                for (Error error : errors) {
                    builder.append(error.getMessageText()).append('\n');
                }
                throw new RuntimeException(builder.toString());
            }
        }
    }

    private void updatePriorityDescriptionAndGeneratePaperwork(CCJobDTO jobDTO, CCJobBatchSessionDTO jobBatchDTO, Messages messages, CCJobBatchSessionServiceResult result, boolean paperwork, boolean returnWarnings) {
// Changes for priority now to be done in core as part of 6.6
        boolean priorityUpdated = updatePriority(jobDTO);
        boolean descriptionUpdated = updateDescription(jobDTO, jobBatchDTO.getInstrumentCode().getValue())

        if (priorityUpdated || descriptionUpdated) {
            makeJobsReadyForUpdate(asList(jobDTO))
            CCJobServiceResult jobUpdateResult = getJobService().update(getSecurityToken(), jobDTO, true);
            MessageUtil.copyMessages(messages, jobUpdateResult);

            MessageUtil.copyMessages(result, messages);
        }

        if (paperwork) {
            getJobPaperworkService().generateReports(getSecurityToken(), jobDTO, returnWarnings);
        }
    }

    private boolean updatePriority(CCJobDTO jobDTO) {
        CCSampleDTO highestPriority = highestPrioritySample(jobDTO);

        // Functionality moved to core 6.6
        //jobDTO.getPriority().setValue(highestPriority.getPriority().getValue());
        //jobDTO.getRequiredDate().setValue(highestPriority.getRequiredDate().getValue());

        return false;
    }

    private CCSampleDTO highestPrioritySample(CCJobDTO jobDTO) {
        return new CCSampleDTO();
    }

    private boolean updateDescription(CCJobDTO jobDTO, String instrumentCode) {
        //String schemes = batchSchemes(jobDTO.getJobId().getValue());
        String schemes = batchSchemesByInstrument(jobDTO.getOrganisationCode().getValue(), jobDTO.getLaboratoryCode().getValue(), jobDTO.getCode().getValue())

        // Leave current description new value is empty
        if (StringUtils.isEmpty(schemes)) {
            return false;
        }

        jobDTO.getDescription().setValue(schemes);
        return true
    }

    protected Query batchSchemesByInstrumentQuery(String orgCode, String labCode, String jobCode) {
        Query query = query(CCSAMPLESCHEMERec.class)
                .innerJoin(CCSAMPLESCHEMERec.sampleFK)
        andEqual(query, CCSAMPLESCHEMERec.organisationCode, orgCode)
        andEqual(query, CCSAMPLESCHEMERec.laboratoryCode, labCode)
        andEqual(query, CCSAMPLESCHEMERec.jobCode, jobCode)

        // Changed as per B-157384; Check duplicates now to also have instrument in description when batched.
        QueryUtil.andEqualAny(query, CCSAMPLERec.primaryAnalyticalType, UNKNOWN, DUPLICATE)

        query.columns([CCSAMPLESCHEMERec.instrumentCode, CCSAMPLESCHEMERec.schemeCode, CCSAMPLESCHEMERec.schemeVersionNumber] as Constraint[])
        query.distinct()
        query.asEntity()
        return query
    }

    private String batchSchemesByInstrument(String orgCode, String labCode, String jobCode) {
        QueryResults results = getTimedEDOIFacade().search(batchSchemesByInstrumentQuery(orgCode, labCode, jobCode))
        List<Rec[]> recs = results.getResults()
        LOGGER.info("batchSchemesByInstrument - recs.size" + CollectionUtils.isEmpty(recs) ? "0" : recs.size().toString())

        Set<String> inst = []
        Set<String> sch = []
        recs.each { Rec[] records ->
            CCSAMPLESCHEMERec rec = (CCSAMPLESCHEMERec) records[0]
            String instCode = rec.getInstrumentCode().trim()
            String schCode = rec.getSchemeCode()

            if (!StringUtils.isEmpty(instCode)) {
                inst.add(instCode)
            }

            sch.add(schCode)
        }

        LOGGER.info("batchSchemesByInstrument - inst" + CollectionUtils.isEmpty(inst) ? "0" : inst.size().toString())
        if (!inst.isEmpty()) {
            return INST_BRACKET_START + inst.join(COMMA_DELIMITER) + INST_BRACKET_END
        }

        LOGGER.info("batchSchemesByInstrument - sch" + CollectionUtils.isEmpty(sch) ? "0" : sch.size().toString())
        if (!sch.isEmpty()) {
            return SCH_BRACKET_START + sch.join(COMMA_DELIMITER) + SCH_BRACKET_END
        }

        return null
    }

//    @Deprecated
//    private String batchSchemes(String jobId) {
//        List<CCJOBSCHEMERec> schemes = getJobSchemeRepository().findJobSchemes(jobId, CCJOBSCHEMERec.schemeCode);
//        List<String> schemeCodes = schemes.collect { it.getSchemeCode() };
//        return listAsString(schemeCodes)
//    }

//    private String listAsString(List<String> schemeCodes) {
//        if (CollectionUtils.isEmpty(schemeCodes)) {
//            return null;
//        }
//
//        return schemeCodes.join(COMMA_DELIMITER);
//    }

    // Need this to set all the "ForUpdate" fields, otherwise call to update service fails
    private void makeJobsReadyForUpdate(List<CCJobDTO> jobs) {
        for (CCJobDTO dto : jobs) {
            dto.getClientCodeForUpdate().setValue(dto.getClientCode().getValue())
            dto.getClientIdForUpdate().setValue(dto.getClientId().getValue())
            dto.getClientContactCodeForUpdate().setValue(dto.getClientContactCode().getValue())
            dto.getClientContactIdForUpdate().setValue(dto.getClientContactId().getValue())
            dto.getClientProjectCodeForUpdate().setValue(dto.getClientProjectCode().getValue())
            dto.getClientProjectIdForUpdate().setValue(dto.getClientProjectId().getValue())
            dto.getErsCodeForUpdate().setValue(dto.getErsCode().getValue())
            dto.getErsItemCodeForUpdate().setValue(dto.getErsItemCode().getValue())
        }
    }

    protected JobRepository getJobRepository() {
        if (repoJob == null) {
            repoJob = getBean(JobRepository.class);
        }
        return repoJob;
    }

    protected SampleRepository getSampleRepository() {
        if (repoSample == null) {
            repoSample = getBean(SampleRepository.class);
        }
        return repoSample;
    }

//    @Deprecated
//    protected JobSchemeRepository getJobSchemeRepository() {
//        if (repoJobScheme == null) {
//            repoJobScheme = getBean(JobSchemeRepository.class);
//        }
//        return repoJobScheme;
//    }

    protected CCJobService getJobService() {
        if (srvJob == null) {
            srvJob = locateJIService(CCJobService.class)
        }
        return srvJob;
    }

    protected CCJobPaperworkService getJobPaperworkService() {
        if (srvJobPW == null) {
            srvJobPW = locateJIService(CCJobPaperworkService.class)
        }
        return srvJobPW
    }

    protected SecurityToken getSecurityToken() {
        if (token == null) {
            token = SecurityTokenServiceLocator.getSecurityTokenService().getSecurityToken()
        }
        return token
    }

    protected TimedEDOIFacade getTimedEDOIFacade() {
        if (tEDOI == null) {
            tEDOI = getBean(TimedEDOIFacade.class)
        }
        return tEDOI
    }
}

/**
 * Created by MYANANG on 4/26/2019.
 */

import static com.mincom.base.InvalidArgumentException.validateNotNull;

public class SampleRecKey {
    private final String organisationCode;
    private final String laboratoryCode;
    private final String jobCode;
    private final String sampleCode;
    private final String sampleId;

    public SampleRecKey(String organisationCode, String laboratoryCode, String jobCode, String sampleCode, String sampleId) {
        validateNotNull(organisationCode, "organisationCode");
        validateNotNull(laboratoryCode, "laboratoryCode");
        validateNotNull(jobCode, "jobCode");
        validateNotNull(sampleCode, "sampleCode");
        this.organisationCode = organisationCode;
        this.laboratoryCode = laboratoryCode;
        this.jobCode = jobCode;
        this.sampleCode = sampleCode;
        this.sampleId = sampleId;
    }

    public String getOrganisationCode() {
        return organisationCode;
    }

    public String getLaboratoryCode() {
        return laboratoryCode;package com.abb.ventyx.test.cclas.m2000.service.ccsample

import com.google.common.collect.Lists
import com.mincom.cclas.domain.sample.SampleRepository
import com.mincom.cclas.domain.schemeversion.SchemeVersionKey
import com.mincom.cclas.query.TimedEDOIFacade
import com.mincom.ellipse.edoi.ejb.ScrollableResults
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTKey
import com.mincom.ellipse.edoi.ejb.ccinstrument.CCINSTRUMENTRec
import com.mincom.ellipse.edoi.ejb.ccinstrumentgroupmember.CCINSTRUMENTGROUPMEMBERRec
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMEKey
import com.mincom.ellipse.edoi.ejb.ccsamplescheme.CCSAMPLESCHEMERec
import com.mincom.ellipse.edoi.ejb.ccscheme.CCSCHEMERec
import com.mincom.ellipse.hook.hooks.CoreServiceHook
import com.mincom.ellipse.types.m2000.instances.CCSampleDTO
import com.mincom.eql.Query
import com.mincom.eql.QueryFactory
import com.mincom.eql.UpdateQuery
import com.mincom.eql.impl.UpdateQueryImpl
import org.apache.commons.lang3.StringUtils

import static com.mincom.cclas.query.QueryResultsCaster.getResult
import static com.mincom.cclas.util.SpringBeanUtil.getBean
import static com.mincom.cclas.util.SystemConstants.NULL_VALUE
import static com.mincom.cclas.util.SystemConstants.YES
import static com.mincom.ellipse.types.base.util.TypeUtil.containsValue
import static java.util.Arrays.asList

public class CCSampleService_createDraft extends CoreServiceHook {
    private SampleRepository sampleRepository = getBean(SampleRepository.class);
    private TimedEDOIFacade edoiFacade = getBean(TimedEDOIFacade.class);

    @Override
    Object onPreExecute(Object inputs) {
        return null
    }

    private Set<CCSAMPLESCHEMERec> buildSourceSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode

        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sampleDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleTemplateCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    public Set<CCSAMPLESCHEMERec> buildDestSSRecSet(CCSampleDTO sampleDTO) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.columns(Lists.newArrayList(
                CCSAMPLESCHEMERec.sampleId,
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeLaboratoryCode,
                CCSAMPLESCHEMERec.schemeVersionNumber,
                CCSAMPLESCHEMERec.instrumentCode,
                CCSAMPLESCHEMERec.instrumentLabCode
        ));
        query.asEntity();

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(sampleDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(sampleDTO.getLaboratoryCode().getValue())));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(sampleDTO.getSampleCode().getValue()));

        Set<CCSAMPLESCHEMERec> ssSet = new HashSet<>();
        ScrollableResults scrollableResults = edoiFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = (CCSAMPLESCHEMERec) scrollableResults.next();
            ssSet.add(ssRec);
        }

        return ssSet;
    }

    private Map<SchemeVersionKey, CCSAMPLESCHEMERec> buildSourceSVKeyAndSSRecMap(CCSampleDTO sampleDTO) {
        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSchemeVersionKeyAndSSRecMap = new HashMap<>();

        Set<CCSAMPLESCHEMERec> sourceSSSet = buildSourceSSRecSet(sampleDTO);
        if (sourceSSSet == null) {
            return;
        }
        for (CCSAMPLESCHEMERec sourceSSRec : sourceSSSet) {
            SchemeVersionKey schemeVersionKey = toSchemeVersionKey(sourceSSRec)

            sourceSchemeVersionKeyAndSSRecMap.putIfAbsent(schemeVersionKey, sourceSSRec);
        }

        return sourceSchemeVersionKeyAndSSRecMap;
    }

    private SchemeVersionKey toSchemeVersionKey(CCSAMPLESCHEMERec sourceSSRec) {
        SchemeVersionKey schemeVersionKey = new SchemeVersionKey(
                sourceSSRec.getOrganisationCode(),
                sanitiseCode(sourceSSRec.getSchemeLaboratoryCode()),
                sourceSSRec.getSchemeCode(),
                sourceSSRec.getSchemeVersionNumber());
        schemeVersionKey
    }


    @Override
    Object onPostExecute(Object input, Object result, Object returnWarnings) {

        CCSampleDTO sampleDTO = input as CCSampleDTO;

        if (sampleDTO == null) {
            return null;
        }

        if (!containsValue(sampleDTO.getLaboratoryCode())) {
            return result
        }

        Set<CCSAMPLESCHEMERec> destLatestSSRecSet = buildDestSSRecSet(sampleDTO);
        if (destLatestSSRecSet == null) {
            return result;
        }

        Map<SchemeVersionKey, CCSAMPLESCHEMERec> sourceSVKeyAndSourceSSRecMap = buildSourceSVKeyAndSSRecMap(sampleDTO);

        List<UpdateQuery> ssUpdateQueries = new ArrayList<>();
        for (CCSAMPLESCHEMERec latestSSRec : destLatestSSRecSet) {

            Query query = getLatestSSRecToUpdateQuery(latestSSRec)
            CCSAMPLESCHEMERec sourceSSRec = sourceSVKeyAndSourceSSRecMap.get(toSchemeVersionKey(latestSSRec));

            if (sourceSSRec == null) {
                throw new RuntimeException("Unable to locate source SSRec based on schemeVersionKey. This must be dev's bug.");
            }

            String instrumentCode = null;
            String instrumentLabCode = null;
            if (!containsValue(sourceSSRec.getInstrumentCode())) {
                String[] results = getInstrumentRecViaInstrumentGroup(sourceSSRec)

                if (results != null && results.length == 2) {
                    instrumentCode = results[0]
                    instrumentLabCode = results[1]
                }
            }

            UpdateQuery updateQuery = new UpdateQueryImpl(query);

            updateQuery.set(CCSAMPLESCHEMERec.instrumentLabCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentLabCode()) ? sourceSSRec.getInstrumentLabCode() : instrumentLabCode))
            updateQuery.set(CCSAMPLESCHEMERec.instrumentCode,
                    sanitiseCode(containsValue(sourceSSRec.getInstrumentCode()) ? sourceSSRec.getInstrumentCode() : instrumentCode))

            ssUpdateQueries.add(updateQuery);
        }

        if (!ssUpdateQueries.isEmpty()) {
            edoiFacade.updateAllBatch(ssUpdateQueries);
        }

        return result
    }

    private String[] getInstrumentRecViaInstrumentGroup(CCSAMPLESCHEMERec sourceSSRec) {
        Query instrumentCodeQuery = QueryFactory.query(CCINSTRUMENTRec.class);
        instrumentCodeQuery
                .columns(asList(CCINSTRUMENTRec.code, CCINSTRUMENTRec.laboratoryCode))
                .and(CCSCHEMERec.instrumentGroupId.equalTo(CCINSTRUMENTGROUPMEMBERRec.instrumentGroupId))
                .and(CCINSTRUMENTGROUPMEMBERRec.instrumentId.equalTo(CCINSTRUMENTKey.id))
                .and(CCSCHEMERec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCSCHEMERec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCSCHEMERec.code.equalTo(sourceSSRec.getSchemeCode()))
                .and(CCINSTRUMENTRec.organisationCode.equalTo(sourceSSRec.getOrganisationCode()))
                .and(CCINSTRUMENTRec.laboratoryCode.equalTo(sourceSSRec.getLaboratoryCode()))
                .and(CCINSTRUMENTRec.isActive.equalTo(YES))
                .orderByAscending(CCINSTRUMENTGROUPMEMBERRec.aix2)
                .setMaxResults(1)

        String[] result = getResult(edoiFacade.scroll(instrumentCodeQuery));
        result
    }

    private Query getLatestSSRecToUpdateQuery(CCSAMPLESCHEMERec latestSSRec) {
        Query query = QueryFactory.query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(latestSSRec.getOrganisationCode()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(sanitiseCode(latestSSRec.getLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(latestSSRec.getJobCode()));
        query.and(CCSAMPLESCHEMERec.sampleCode.equalTo(latestSSRec.getSampleCode()));
        query.and(CCSAMPLESCHEMERec.schemeLaboratoryCode.equalTo(sanitiseCode(latestSSRec.getSchemeLaboratoryCode())));
        query.and(CCSAMPLESCHEMERec.schemeCode.equalTo(latestSSRec.getSchemeCode()));
        query.and(CCSAMPLESCHEMERec.schemeVersionNumber.equalTo(latestSSRec.getSchemeVersionNumber()));
        query
    }

    private String sanitiseCode(String value) {
        if (StringUtils.isBlank(value)) {
            return NULL_VALUE;
        } else {
            return value;
        }
    }
}
    }

    public String getJobCode() {
        return jobCode;
    }

    public String getSampleCode() {
        return sampleCode;
    }

    public String getSampleId() {
        return sampleId;
    }
/*
    @Override
    public int hashCode() {
        int result = schemeCode.hashCode();
        result = 31 * result + analyteCode.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SchemeAnalyteKey{" +
                "schemeCode='" + schemeCode + '\'' +
                ", analyteCode='" + analyteCode + '\'' +
                '}';
    }*/
}

            Object[] row = scrollableResults.next();

            String schemeGroupCode = row[0];
            String schemeCode = row[1];

            Set<String> schemeCodes = schemeGroupAndSchemesMap.get(schemeGroupCode);
            if (schemeCodes == null) {
                schemeCodes = new HashSet<>();
                schemeGroupAndSchemesMap.put(schemeGroupCode, schemeCodes);
            }
            schemeCodes.add(schemeCode);
        }

        schemeGroupAndSchemesMap
    }

    private Map<String, Set<CCREPORTTEMPLATERec>> findSchemeGroupCodeAndReportTemplatesMap(Set<String> schemeGroupCodes) {
        Map<String, Set<CCREPORTTEMPLATERec>> schemeGroupAndReportTemplatesMap = new HashMap<>();
        if (schemeGroupCodes.isEmpty()) {
            return schemeGroupAndReportTemplatesMap;
        }

        Query query = query(CCREPORTTEMPLATERec.class)
        query.join(CCREPORTTEMPLATEKey.id, MSF0P5Rec.entityKey);
        query.and(MSF0P5Rec.entityType.equalTo("CCReportTemplateService.CCReportTemplate.ReportSchemeGroup"));
        query.and(equalAny(MSF0P5Rec.propertyValue, schemeGroupCodes));
        query.and(CCREPORTTEMPLATERec.organisationCode.equalTo(securityContextService.contextOrganisationCode()));
        query.and(CCREPORTTEMPLATERec.laboratoryCode.in(NULL_VALUE, securityContextService.contextLaboratoryCode()));
        query.and(CCREPORTTEMPLATERec.isActive.equalTo(YES));
        query.and(CCREPORTTEMPLATERec.reportType.in(ReportTemplateReportTypeEnum.PAPERWORK.value(), ReportTemplateReportTypeEnum.LABELS.value()));

        query.columns(newArrayList(
                MSF0P5Rec.propertyValue,//0
                CCREPORTTEMPLATEKey.id, //1
                CCREPORTTEMPLATERec.code,  //2
                CCREPORTTEMPLATERec.outputFileNameSyntaxId,//3
                CCREPORTTEMPLATERec.defaultOutputFormat, //4
                CCREPORTTEMPLATERec.deviceId //5
        ));

        ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
        while (scrollableResults.hasNext()) {
            Object[] rows = scrollableResults.next();

            String schemeGroupCode = rows[0];

            CCREPORTTEMPLATERec reportTemplateRec = new CCREPORTTEMPLATERec();
            reportTemplateRec.setPrimaryKey(new CCREPORTTEMPLATEKey((String) rows[1]));
            reportTemplateRec.setCode((String) rows[2])
            reportTemplateRec.setOutputFileNameSyntaxId((String) rows[3])
            reportTemplateRec.setDefaultOutputFormat((String) rows[4])
            reportTemplateRec.setDeviceId((String) rows[5])

            Set<CCREPORTTEMPLATERec> rrRecs = schemeGroupAndReportTemplatesMap.get(schemeGroupCode);
            if (rrRecs == null) {
                rrRecs = new HashSet<>();
                schemeGroupAndReportTemplatesMap.put(schemeGroupCode, rrRecs);
            }

            rrRecs.add(reportTemplateRec);
        }

        schemeGroupAndReportTemplatesMap;
    }

    private Map<String, Set<CCSAMPLESCHEMERec>> findSchemeAndSampleSchemesMap(CCJobDTO jobDTO) {
        Query query = query(CCSAMPLESCHEMERec.class);

        query.and(CCSAMPLESCHEMERec.organisationCode.equalTo(jobDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLESCHEMERec.laboratoryCode.equalTo(jobDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLESCHEMERec.jobCode.equalTo(jobDTO.getCode().getValue()));

        query.columns(newArrayList(
                CCSAMPLESCHEMEKey.id,
                CCSAMPLESCHEMERec.organisationCode,
                CCSAMPLESCHEMERec.laboratoryCode,
                CCSAMPLESCHEMERec.jobCode,
                CCSAMPLESCHEMERec.sampleCode,
                CCSAMPLESCHEMERec.schemeCode,
                CCSAMPLESCHEMERec.schemeVersionNumber
        ))

        query.asEntity();

        ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
        Map<String, Set<CCSAMPLESCHEMERec>> schemeAndSampleSchemesMap = new HashMap<>();
        while (scrollableResults.hasNext()) {
            CCSAMPLESCHEMERec ssRec = scrollableResults.next() as CCSAMPLESCHEMERec;

            Set<CCSAMPLESCHEMERec> ssSet = schemeAndSampleSchemesMap.get(ssRec.getSchemeCode());
            if (ssSet == null) {
                ssSet = new HashSet<>();
                schemeAndSampleSchemesMap.put(ssRec.getSchemeCode(), ssSet);
            }
            ssSet.add(ssRec);
        }
        schemeAndSampleSchemesMap
    }

    private Map<String, String> findDeviceIdAndCodeMap(Collection<Set<CCREPORTTEMPLATERec>> reportTemplateSetList) {
        Set<String> deviceIds = new HashSet<>();

        for (Set<CCREPORTTEMPLATERec> reportTemplateSet : reportTemplateSetList) {
            for (CCREPORTTEMPLATERec rr : reportTemplateSet) {
                if (isNotBlank(rr.deviceId)) {
                    deviceIds.add(rr.deviceId);
                }
            }
        }

        Map<String, String> deviceIdAndCodeMap = new HashMap<>();
        if (!deviceIds.isEmpty()) {
            Query query = query(CCDEVICERec.class);
            query.and(equalAny(CCDEVICEKey.id, deviceIds));
            query.columns(newArrayList(CCDEVICEKey.id, CCDEVICERec.code))

            ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
            while (scrollableResults.hasNext()) {
                Object[] row = scrollableResults.next();

                deviceIdAndCodeMap.put((String) row[0], (String) row[1]);
            }
        }

        deviceIdAndCodeMap;
    }

    private void createSchemeGroupBasedPaperworkOrLabels(CCJobDTO jobDTO, Messages messages) {
        JobRecKey jobRecKey = new JobRecKey(jobDTO.getOrganisationCode().getValue(), jobDTO.getLaboratoryCode().getValue(),
                jobDTO.getCode().getValue());

        Set<CCJOBSCHEMERec> schemes = jobRepository.findJobSchemes(newHashSet(jobRecKey));
        if (schemes.isEmpty()) {
            return;
        }

        Map<String, Set<String>> schemeGroupCodeAndSchemeCodesMap = findSchemeGroupAndSchemesMap(schemes);
        Map<String, Set<CCREPORTTEMPLATERec>> schemeGroupCodeAndReportTemplatesMap = findSchemeGroupCodeAndReportTemplatesMap(schemeGroupCodeAndSchemeCodesMap.keySet());
        Map<String, Set<CCSAMPLESCHEMERec>> schemeAndSampleSchemesMap = findSchemeAndSampleSchemesMap(jobDTO);

        Map<String, String> deviceIdAndCodeMap = findDeviceIdAndCodeMap(schemeGroupCodeAndReportTemplatesMap.values());

        Map<String, CCSAMPLERec> sampleMap = findSampleMap(jobDTO);

        List<CCReportRequestDTO> reportRequestDTOList = new ArrayList<>();
        List<ReportRequestOutput> reportRequestOutputList = new ArrayList<>();

        for (Map.Entry<String, Set<CCREPORTTEMPLATERec>> entry : schemeGroupCodeAndReportTemplatesMap.entrySet()) {
            String schemeGroupCode = entry.getKey();
            Set<CCREPORTTEMPLATERec> reportTemplates = entry.getValue();

            Set<String> schemeCodes = schemeGroupCodeAndSchemeCodesMap.get(schemeGroupCode);
            if (schemeCodes != null) {
                for (String schemeCode : schemeCodes) {
                    Set<CCSAMPLESCHEMERec> ssRecs = schemeAndSampleSchemesMap.get(schemeCode);

                    for (CCREPORTTEMPLATERec reportTemplate : reportTemplates) {
                        ReportRequestBuilder rrBuilder = reportRequestServiceForRRBuilder.createBuilder(reportTemplate.getCode())
                        rrBuilder.setRequiredNewTransaction(false);
                        rrBuilder.setReportRequestType(new ReportRequestReportType(reportTemplate.getReportType()));

                        rrBuilder.setReportRequestCodeSyntaxCode("REP_REQ_CODE_SYNTAX")
                        rrBuilder.setReportRequestNameSyntaxCode("REP_REQ_NAME_SYNTAX")

                        String deviceCode = deviceIdAndCodeMap.get(reportTemplate.getDeviceId());
                        if (isNotBlank(deviceCode)) {
                            rrBuilder.setNumberPrinterCopies(1)
                            rrBuilder.setPrinterCode(deviceCode);
                        }

                        rrBuilder.addJob(jobDTO.getCode().getValue());

                        for (CCSAMPLESCHEMERec ss : ssRecs) {
                            rrBuilder.addSample(jobDTO.getCode().getValue(), ss.getSampleCode(), ReportableRule.None);
                        }
                        rrBuilder.addSchemesForJobWithName(jobDTO.getCode().getValue(), ReportableRule.None, schemeCode);

                        ReportRequestOutput reportRequestOutput = rrBuilder.build(null);
                        CCReportRequestDTO reportRequestDTO = reportRequestOutput.getReportRequest()

                        reportRequestDTOList.add(reportRequestDTO);
                        updateReportRequestSampleReportableFlags(reportRequestDTO, ssRecs, sampleMap);

                        reportRequestOutputList.add(reportRequestOutput);
                    }
                }
            }
        }

        updateReportRequestName(reportRequestDTOList, jobDTO);
        generateReport(reportRequestOutputList);
    }

    private void generateReport(List<ReportRequestOutput> reportRequestOutputList) {
        for (ReportRequestOutput reportRequestOutput : reportRequestOutputList) {
            reportGenerator.generateReport(null, reportRequestOutput, false);
        }
    }

    private Map<String, CCSAMPLERec> findSampleMap(CCJobDTO jobDTO) {
        Query query = query(CCSAMPLERec.class);

        query.and(CCSAMPLERec.organisationCode.equalTo(jobDTO.getOrganisationCode().getValue()));
        query.and(CCSAMPLERec.laboratoryCode.equalTo(jobDTO.getLaboratoryCode().getValue()));
        query.and(CCSAMPLERec.jobCode.equalTo(jobDTO.getCode().getValue()));

        query.columns(newArrayList(
                CCSAMPLEKey.id,
                CCSAMPLERec.organisationCode,
                CCSAMPLERec.laboratoryCode,
                CCSAMPLERec.jobCode,
                CCSAMPLERec.code,
                CCSAMPLERec.isGenerateLabel,
                CCSAMPLERec.isGeneratePaperwork
        ))
        query.asEntity();

        ScrollableResults scrollableResults = timedEDOIFacade.scroll(query);
        Map<String, CCSAMPLERec> sampleMap = new HashMap();
        while (scrollableResults.hasNext()) {
            CCSAMPLERec sampleRec = (CCSAMPLERec) scrollableResults.next();
            sampleMap.put(sampleRec.getCode(), sampleRec);
        }
        sampleMap;
    }

    private void updateReportRequestSampleReportableFlags(CCReportRequestDTO reportRequestDTO,
                                                          Set<CCSAMPLESCHEMERec> ssRecs,
                                                          Map<String, CCSAMPLERec> sampleMap) {
        if (ssRecs.isEmpty()) {
            return;
        }

        List<UpdateQuery> updateQueries = new ArrayList<>();

        for (CCSAMPLESCHEMERec ssRec : ssRecs) {
            Query rrSampleQuery = query(CCREPREQSAMPLERec.class);
            rrSampleQuery.and(CCREPREQSAMPLERec.organisationCode.equalTo(ssRec.getOrganisationCode()));
            rrSampleQuery.and(CCREPREQSAMPLERec.laboratoryCode.equalTo(ssRec.getLaboratoryCode()));
            rrSampleQuery.and(CCREPREQSAMPLERec.repReqCode.equalTo(reportRequestDTO.getCode().getValue()));

            rrSampleQuery.and(CCREPREQSAMPLERec.jobCode.equalTo(ssRec.getJobCode()));
            rrSampleQuery.and(CCREPREQSAMPLERec.sampleCode.equalTo(ssRec.getSampleCode()));

            UpdateQuery updateQuery = new UpdateQueryImpl(rrSampleQuery);

            CCSAMPLERec sample = sampleMap.get(ssRec.getSampleCode());

            if (ReportTemplateReportTypeEnum.LABELS.value().equals(reportRequestDTO.getReportType().getValue())) {
                updateQuery.set(CCREPREQSAMPLERec.isReportable, sample.getIsGenerateLabel());
            } else if (ReportTemplateReportTypeEnum.PAPERWORK.value().equals(reportRequestDTO.getReportType().getValue())) {
                updateQuery.set(CCREPREQSAMPLERec.isReportable, sample.getIsGeneratePaperwork());
            } else {
                throw new RuntimeException("Un-expected reportType - " + reportRequestDTO.getReportType().getValue());
            }
            updateQueries.add(updateQuery);
        }

        timedEDOIFacade.updateAllBatch(updateQueries);
    }

    private void updateReportRequestName(List<CCReportRequestDTO> reportRequestDTOList, CCJobDTO jobDTO) {
        if (reportRequestDTOList.isEmpty()) {
            return;
        }

        //change the generated report request name to be the same as batch job name.
        int index = 1;
        for (CCReportRequestDTO reportRequestDTO : reportRequestDTOList) {
            reportRequestDTO.getName().setValue(jobDTO.getName().getValue() + "_" + index);
            index++;
        }

        CCReportRequestServiceResult[] results = reportRequestService.multipleUpdate(securityContextService.securityToken(),
                reportRequestDTOList.toArray(new CCReportRequestDTO[reportRequestDTOList.size()]), false);

        throwExceptionIfAnyError(results);
    }

    private static <T extends ServiceResult> void throwExceptionIfAnyError(T[] results) {
        for (ServiceResult result : results) {
            if (result.hasErrors()) {
                Error[] errors = result.getErrors()

                StringBuilder builder = new StringBuilder();
                for (Error error : errors) {
                    builder.append(error.getMessageText()).append('\n');
                }
                throw new RuntimeException(builder.toString());
            }
        }
    }

    private void updatePriorityDescriptionAndGeneratePaperwork(CCJobDTO jobDTO, CCJobBatchSessionDTO jobBatchDTO, Messages messages, CCJobBatchSessionServiceResult result, boolean paperwork, boolean returnWarnings) {
// Changes for priority now to be done in core as part of 6.6
        boolean priorityUpdated = updatePriority(jobDTO);
        boolean descriptionUpdated = updateDescription(jobDTO, jobBatchDTO.getInstrumentCode().getValue())

        if (priorityUpdated || descriptionUpdated) {
            makeJobsReadyForUpdate(asList(jobDTO))
            CCJobServiceResult jobUpdateResult = getJobService().update(getSecurityToken(), jobDTO, true);
            MessageUtil.copyMessages(messages, jobUpdateResult);

            MessageUtil.copyMessages(result, messages);
        }

        if (paperwork) {
            getJobPaperworkService().generateReports(getSecurityToken(), jobDTO, returnWarnings);
        }
    }

    private boolean updatePriority(CCJobDTO jobDTO) {
        CCSampleDTO highestPriority = highestPrioritySample(jobDTO);

        // Functionality moved to core 6.6
        //jobDTO.getPriority().setValue(highestPriority.getPriority().getValue());
        //jobDTO.getRequiredDate().setValue(highestPriority.getRequiredDate().getValue());

        return false;
    }

    private CCSampleDTO highestPrioritySample(CCJobDTO jobDTO) {
        return new CCSampleDTO();
    }

    private boolean updateDescription(CCJobDTO jobDTO, String instrumentCode) {
        //String schemes = batchSchemes(jobDTO.getJobId().getValue());
        String schemes = batchSchemesByInstrument(jobDTO.getOrganisationCode().getValue(), jobDTO.getLaboratoryCode().getValue(), jobDTO.getCode().getValue())

        // Leave current description new value is empty
        if (StringUtils.isEmpty(schemes)) {
            return false;
        }

        jobDTO.getDescription().setValue(schemes);
        return true
    }

    protected Query batchSchemesByInstrumentQuery(String orgCode, String labCode, String jobCode) {
        Query query = query(CCSAMPLESCHEMERec.class)
                .innerJoin(CCSAMPLESCHEMERec.sampleFK)
        andEqual(query, CCSAMPLESCHEMERec.organisationCode, orgCode)
        andEqual(query, CCSAMPLESCHEMERec.laboratoryCode, labCode)
        andEqual(query, CCSAMPLESCHEMERec.jobCode, jobCode)

        // Changed as per B-157384; Check duplicates now to also have instrument in description when batched.
        QueryUtil.andEqualAny(query, CCSAMPLERec.primaryAnalyticalType, UNKNOWN, DUPLICATE)

        query.columns([CCSAMPLESCHEMERec.instrumentCode, CCSAMPLESCHEMERec.schemeCode, CCSAMPLESCHEMERec.schemeVersionNumber] as Constraint[])
        query.distinct()
        query.asEntity()
        return query
    }

    private String batchSchemesByInstrument(String orgCode, String labCode, String jobCode) {
        QueryResults results = getTimedEDOIFacade().search(batchSchemesByInstrumentQuery(orgCode, labCode, jobCode))
        List<Rec[]> recs = results.getResults()
        LOGGER.info("batchSchemesByInstrument - recs.size" + CollectionUtils.isEmpty(recs) ? "0" : recs.size().toString())

        Set<String> inst = []
        Set<String> sch = []
        recs.each { Rec[] records ->
            CCSAMPLESCHEMERec rec = (CCSAMPLESCHEMERec) records[0]
            String instCode = rec.getInstrumentCode().trim()
            String schCode = rec.getSchemeCode()

            if (!StringUtils.isEmpty(instCode)) {
                inst.add(instCode)
            }

            sch.add(schCode)
        }

        LOGGER.info("batchSchemesByInstrument - inst" + CollectionUtils.isEmpty(inst) ? "0" : inst.size().toString())
        if (!inst.isEmpty()) {
            return INST_BRACKET_START + inst.join(COMMA_DELIMITER) + INST_BRACKET_END
        }

        LOGGER.info("batchSchemesByInstrument - sch" + CollectionUtils.isEmpty(sch) ? "0" : sch.size().toString())
        if (!sch.isEmpty()) {
            return SCH_BRACKET_START + sch.join(COMMA_DELIMITER) + SCH_BRACKET_END
        }

        return null
    }

//    @Deprecated
//    private String batchSchemes(String jobId) {
//        List<CCJOBSCHEMERec> schemes = getJobSchemeRepository().findJobSchemes(jobId, CCJOBSCHEMERec.schemeCode);
//        List<String> schemeCodes = schemes.collect { it.getSchemeCode() };
//        return listAsString(schemeCodes)
//    }

//    private String listAsString(List<String> schemeCodes) {
//        if (CollectionUtils.isEmpty(schemeCodes)) {
//            return null;
//        }
//
//        return schemeCodes.join(COMMA_DELIMITER);
//    }

    // Need this to set all the "ForUpdate" fields, otherwise call to update service fails
    private void makeJobsReadyForUpdate(List<CCJobDTO> jobs) {
        for (CCJobDTO dto : jobs) {
            dto.getClientCodeForUpdate().setValue(dto.getClientCode().getValue())
            dto.getClientIdForUpdate().setValue(dto.getClientId().getValue())
            dto.getClientContactCodeForUpdate().setValue(dto.getClientContactCode().getValue())
            dto.getClientContactIdForUpdate().setValue(dto.getClientContactId().getValue())
            dto.getClientProjectCodeForUpdate().setValue(dto.getClientProjectCode().getValue())
            dto.getClientProjectIdForUpdate().setValue(dto.getClientProjectId().getValue())
            dto.getErsCodeForUpdate().setValue(dto.getErsCode().getValue())
            dto.getErsItemCodeForUpdate().setValue(dto.getErsItemCode().getValue())
        }
    }

    protected JobRepository getJobRepository() {
        if (repoJob == null) {
            repoJob = getBean(JobRepository.class);
        }
        return repoJob;
    }

    protected SampleRepository getSampleRepository() {
        if (repoSample == null) {
            repoSample = getBean(SampleRepository.class);
        }
        return repoSample;
    }

//    @Deprecated
//    protected JobSchemeRepository getJobSchemeRepository() {
//        if (repoJobScheme == null) {
//            repoJobScheme = getBean(JobSchemeRepository.class);
//        }
//        return repoJobScheme;
//    }

    protected CCJobService getJobService() {
        if (srvJob == null) {
            srvJob = locateJIService(CCJobService.class)
        }
        return srvJob;
    }

    protected CCJobPaperworkService getJobPaperworkService() {
        if (srvJobPW == null) {
            srvJobPW = locateJIService(CCJobPaperworkService.class)
        }
        return srvJobPW
    }

    protected SecurityToken getSecurityToken() {
        if (token == null) {
            token = SecurityTokenServiceLocator.getSecurityTokenService().getSecurityToken()
        }
        return token
    }

    protected TimedEDOIFacade getTimedEDOIFacade() {
        if (tEDOI == null) {
            tEDOI = getBean(TimedEDOIFacade.class)
        }
        return tEDOI
    }
}
