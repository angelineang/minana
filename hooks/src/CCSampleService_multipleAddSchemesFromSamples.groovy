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