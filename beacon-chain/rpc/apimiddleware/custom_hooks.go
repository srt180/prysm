package apimiddleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prysmaticlabs/prysm/v3/api/gateway/apimiddleware"
	"github.com/prysmaticlabs/prysm/v3/config/params"
	types "github.com/prysmaticlabs/prysm/v3/consensus-types/primitives"
	ethpbv2 "github.com/prysmaticlabs/prysm/v3/proto/eth/v2"
	"github.com/prysmaticlabs/prysm/v3/time/slots"
)

// https://ethereum.github.io/beacon-apis/#/Validator/prepareBeaconProposer expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct.
func wrapFeeRecipientsArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*FeeRecipientsRequestJSON); !ok {
		return true, nil
	}
	recipients := make([]*FeeRecipientJson, 0)
	if err := json.NewDecoder(req.Body).Decode(&recipients); err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
	}
	j := &FeeRecipientsRequestJSON{Recipients: recipients}
	b, err := json.Marshal(j)
	if err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
	}
	req.Body = io.NopCloser(bytes.NewReader(b))
	return true, nil
}

// https://ethereum.github.io/beacon-APIs/#/Validator/registerValidator expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct.
func wrapSignedValidatorRegistrationsArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*SignedValidatorRegistrationsRequestJson); !ok {
		return true, nil
	}
	registrations := make([]*SignedValidatorRegistrationJson, 0)
	if err := json.NewDecoder(req.Body).Decode(&registrations); err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
	}
	j := &SignedValidatorRegistrationsRequestJson{Registrations: registrations}
	b, err := json.Marshal(j)
	if err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
	}
	req.Body = io.NopCloser(bytes.NewReader(b))
	return true, nil
}

// https://ethereum.github.io/beacon-apis/#/Beacon/submitPoolAttestations expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct with a 'data' field.
func wrapAttestationsArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*SubmitAttestationRequestJson); ok {
		atts := make([]*AttestationJson, 0)
		if err := json.NewDecoder(req.Body).Decode(&atts); err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
		}
		j := &SubmitAttestationRequestJson{Data: atts}
		b, err := json.Marshal(j)
		if err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return true, nil
}

// Some endpoints e.g. https://ethereum.github.io/beacon-apis/#/Validator/getAttesterDuties expect posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct with an 'Index' field.
func wrapValidatorIndicesArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*DutiesRequestJson); ok {
		indices := make([]string, 0)
		if err := json.NewDecoder(req.Body).Decode(&indices); err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
		}
		j := &DutiesRequestJson{Index: indices}
		b, err := json.Marshal(j)
		if err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return true, nil
}

// https://ethereum.github.io/beacon-apis/#/Validator/publishAggregateAndProofs expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct with a 'data' field.
func wrapSignedAggregateAndProofArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*SubmitAggregateAndProofsRequestJson); ok {
		data := make([]*SignedAggregateAttestationAndProofJson, 0)
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
		}
		j := &SubmitAggregateAndProofsRequestJson{Data: data}
		b, err := json.Marshal(j)
		if err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return true, nil
}

// https://ethereum.github.io/beacon-apis/#/Validator/prepareBeaconCommitteeSubnet expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct with a 'data' field.
func wrapBeaconCommitteeSubscriptionsArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*SubmitBeaconCommitteeSubscriptionsRequestJson); ok {
		data := make([]*BeaconCommitteeSubscribeJson, 0)
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
		}
		j := &SubmitBeaconCommitteeSubscriptionsRequestJson{Data: data}
		b, err := json.Marshal(j)
		if err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return true, nil
}

// https://ethereum.github.io/beacon-APIs/#/Validator/prepareSyncCommitteeSubnets expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct with a 'data' field.
func wrapSyncCommitteeSubscriptionsArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*SubmitSyncCommitteeSubscriptionRequestJson); ok {
		data := make([]*SyncCommitteeSubscriptionJson, 0)
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
		}
		j := &SubmitSyncCommitteeSubscriptionRequestJson{Data: data}
		b, err := json.Marshal(j)
		if err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return true, nil
}

// https://ethereum.github.io/beacon-APIs/#/Beacon/submitPoolSyncCommitteeSignatures expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct with a 'data' field.
func wrapSyncCommitteeSignaturesArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*SubmitSyncCommitteeSignaturesRequestJson); ok {
		data := make([]*SyncCommitteeMessageJson, 0)
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
		}
		j := &SubmitSyncCommitteeSignaturesRequestJson{Data: data}
		b, err := json.Marshal(j)
		if err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return true, nil
}

// https://ethereum.github.io/beacon-APIs/#/Validator/publishContributionAndProofs expects posting a top-level array.
// We make it more proto-friendly by wrapping it in a struct with a 'data' field.
func wrapSignedContributionAndProofsArray(
	endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	if _, ok := endpoint.PostRequest.(*SubmitContributionAndProofsRequestJson); ok {
		data := make([]*SignedContributionAndProofJson, 0)
		if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not decode body")
		}
		j := &SubmitContributionAndProofsRequestJson{Data: data}
		b, err := json.Marshal(j)
		if err != nil {
			return false, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal wrapped body")
		}
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return true, nil
}

type phase0PublishBlockRequestJson struct {
	Phase0Block *BeaconBlockJson `json:"phase0_block"`
	Signature   string           `json:"signature" hex:"true"`
}

type altairPublishBlockRequestJson struct {
	AltairBlock *BeaconBlockAltairJson `json:"altair_block"`
	Signature   string                 `json:"signature" hex:"true"`
}

type bellatrixPublishBlockRequestJson struct {
	BellatrixBlock *BeaconBlockBellatrixJson `json:"bellatrix_block"`
	Signature      string                    `json:"signature" hex:"true"`
}

type bellatrixPublishBlindedBlockRequestJson struct {
	BellatrixBlock *BlindedBeaconBlockBellatrixJson `json:"bellatrix_block"`
	Signature      string                           `json:"signature" hex:"true"`
}

// setInitialPublishBlockPostRequest is triggered before we deserialize the request JSON into a struct.
// We don't know which version of the block got posted, but we can determine it from the slot.
// We know that blocks of all versions have a Message field with a Slot field,
// so we deserialize the request into a struct s, which has the right fields, to obtain the slot.
// Once we know the slot, we can determine what the PostRequest field of the endpoint should be, and we set it appropriately.
func setInitialPublishBlockPostRequest(endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	s := struct {
		Message struct {
			Slot string
		}
	}{}

	buf, err := io.ReadAll(req.Body)
	if err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not read body")
	}
	if err := json.Unmarshal(buf, &s); err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not read slot from body")
	}
	slot, err := strconv.ParseUint(s.Message.Slot, 10, 64)
	if err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "slot is not an unsigned integer")
	}
	currentEpoch := slots.ToEpoch(types.Slot(slot))
	if currentEpoch < params.BeaconConfig().AltairForkEpoch {
		endpoint.PostRequest = &SignedBeaconBlockContainerJson{}
	} else if currentEpoch < params.BeaconConfig().BellatrixForkEpoch {
		endpoint.PostRequest = &SignedBeaconBlockAltairContainerJson{}
	} else {
		endpoint.PostRequest = &SignedBeaconBlockBellatrixContainerJson{}
	}
	req.Body = io.NopCloser(bytes.NewBuffer(buf))
	return true, nil
}

// In preparePublishedBlock we transform the PostRequest.
// gRPC expects an XXX_block field in the JSON object, but we have a message field at this point.
// We do a simple conversion depending on the type of endpoint.PostRequest
// (which was filled out previously in setInitialPublishBlockPostRequest).
func preparePublishedBlock(endpoint *apimiddleware.Endpoint, _ http.ResponseWriter, _ *http.Request) apimiddleware.ErrorJson {
	if block, ok := endpoint.PostRequest.(*SignedBeaconBlockContainerJson); ok {
		// Prepare post request that can be properly decoded on gRPC side.
		actualPostReq := &phase0PublishBlockRequestJson{
			Phase0Block: block.Message,
			Signature:   block.Signature,
		}
		endpoint.PostRequest = actualPostReq
		return nil
	}
	if block, ok := endpoint.PostRequest.(*SignedBeaconBlockAltairContainerJson); ok {
		// Prepare post request that can be properly decoded on gRPC side.
		actualPostReq := &altairPublishBlockRequestJson{
			AltairBlock: block.Message,
			Signature:   block.Signature,
		}
		endpoint.PostRequest = actualPostReq
		return nil
	}
	if block, ok := endpoint.PostRequest.(*SignedBeaconBlockBellatrixContainerJson); ok {
		// Prepare post request that can be properly decoded on gRPC side.
		actualPostReq := &bellatrixPublishBlockRequestJson{
			BellatrixBlock: block.Message,
			Signature:      block.Signature,
		}
		endpoint.PostRequest = actualPostReq
		return nil
	}
	return apimiddleware.InternalServerError(errors.New("unsupported block type"))
}

// setInitialPublishBlindedBlockPostRequest is triggered before we deserialize the request JSON into a struct.
// We don't know which version of the block got posted, but we can determine it from the slot.
// We know that blocks of all versions have a Message field with a Slot field,
// so we deserialize the request into a struct s, which has the right fields, to obtain the slot.
// Once we know the slot, we can determine what the PostRequest field of the endpoint should be, and we set it appropriately.
func setInitialPublishBlindedBlockPostRequest(endpoint *apimiddleware.Endpoint,
	_ http.ResponseWriter,
	req *http.Request,
) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	s := struct {
		Message struct {
			Slot string
		}
	}{}

	buf, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not read body")
	}
	if err := json.Unmarshal(buf, &s); err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not read slot from body")
	}
	slot, err := strconv.ParseUint(s.Message.Slot, 10, 64)
	if err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "slot is not an unsigned integer")
	}
	currentEpoch := slots.ToEpoch(types.Slot(slot))
	if currentEpoch < params.BeaconConfig().AltairForkEpoch {
		endpoint.PostRequest = &SignedBeaconBlockContainerJson{}
	} else if currentEpoch < params.BeaconConfig().BellatrixForkEpoch {
		endpoint.PostRequest = &SignedBeaconBlockAltairContainerJson{}
	} else {
		endpoint.PostRequest = &SignedBlindedBeaconBlockBellatrixContainerJson{}
	}
	req.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
	return true, nil
}

// In preparePublishedBlindedBlock we transform the PostRequest.
// gRPC expects either an XXX_block field in the JSON object, but we have a message field at this point.
// We do a simple conversion depending on the type of endpoint.PostRequest
// (which was filled out previously in setInitialPublishBlockPostRequest).
func preparePublishedBlindedBlock(endpoint *apimiddleware.Endpoint, _ http.ResponseWriter, _ *http.Request) apimiddleware.ErrorJson {
	if block, ok := endpoint.PostRequest.(*SignedBeaconBlockContainerJson); ok {
		// Prepare post request that can be properly decoded on gRPC side.
		actualPostReq := &phase0PublishBlockRequestJson{
			Phase0Block: block.Message,
			Signature:   block.Signature,
		}
		endpoint.PostRequest = actualPostReq
		return nil
	}
	if block, ok := endpoint.PostRequest.(*SignedBeaconBlockAltairContainerJson); ok {
		// Prepare post request that can be properly decoded on gRPC side.
		actualPostReq := &altairPublishBlockRequestJson{
			AltairBlock: block.Message,
			Signature:   block.Signature,
		}
		endpoint.PostRequest = actualPostReq
		return nil
	}
	if block, ok := endpoint.PostRequest.(*SignedBlindedBeaconBlockBellatrixContainerJson); ok {
		// Prepare post request that can be properly decoded on gRPC side.
		actualPostReq := &bellatrixPublishBlindedBlockRequestJson{
			BellatrixBlock: block.Message,
			Signature:      block.Signature,
		}
		endpoint.PostRequest = actualPostReq
		return nil
	}
	return apimiddleware.InternalServerError(errors.New("unsupported block type"))
}

type tempSyncCommitteesResponseJson struct {
	Data *tempSyncCommitteeValidatorsJson `json:"data"`
}

type tempSyncCommitteeValidatorsJson struct {
	Validators          []string                              `json:"validators"`
	ValidatorAggregates []*tempSyncSubcommitteeValidatorsJson `json:"validator_aggregates"`
}

type tempSyncSubcommitteeValidatorsJson struct {
	Validators []string `json:"validators"`
}

// https://ethereum.github.io/beacon-APIs/?urls.primaryName=v2.0.0#/Beacon/getEpochSyncCommittees returns validator_aggregates as a nested array.
// grpc-gateway returns a struct with nested fields which we have to transform into a plain 2D array.
func prepareValidatorAggregates(body []byte, responseContainer interface{}) (apimiddleware.RunDefault, apimiddleware.ErrorJson) {
	tempContainer := &tempSyncCommitteesResponseJson{}
	if err := json.Unmarshal(body, tempContainer); err != nil {
		return false, apimiddleware.InternalServerErrorWithMessage(err, "could not unmarshal response into temp container")
	}
	container, ok := responseContainer.(*SyncCommitteesResponseJson)
	if !ok {
		return false, apimiddleware.InternalServerError(errors.New("container is not of the correct type"))
	}

	container.Data = &SyncCommitteeValidatorsJson{}
	container.Data.Validators = tempContainer.Data.Validators
	container.Data.ValidatorAggregates = make([][]string, len(tempContainer.Data.ValidatorAggregates))
	for i, srcValAgg := range tempContainer.Data.ValidatorAggregates {
		dstValAgg := make([]string, len(srcValAgg.Validators))
		copy(dstValAgg, tempContainer.Data.ValidatorAggregates[i].Validators)
		container.Data.ValidatorAggregates[i] = dstValAgg
	}

	return false, nil
}

type phase0BlockResponseJson struct {
	Version             string                          `json:"version"`
	Data                *SignedBeaconBlockContainerJson `json:"data"`
	ExecutionOptimistic bool                            `json:"execution_optimistic"`
}

type altairBlockResponseJson struct {
	Version             string                                `json:"version"`
	Data                *SignedBeaconBlockAltairContainerJson `json:"data"`
	ExecutionOptimistic bool                                  `json:"execution_optimistic"`
}

type bellatrixBlockResponseJson struct {
	Version             string                                   `json:"version"`
	Data                *SignedBeaconBlockBellatrixContainerJson `json:"data"`
	ExecutionOptimistic bool                                     `json:"execution_optimistic"`
}

func serializeV2Block(response interface{}) (apimiddleware.RunDefault, []byte, apimiddleware.ErrorJson) {
	respContainer, ok := response.(*BlockV2ResponseJson)
	if !ok {
		return false, nil, apimiddleware.InternalServerError(errors.New("container is not of the correct type"))
	}

	var actualRespContainer interface{}
	switch {
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_PHASE0.String())):
		actualRespContainer = &phase0BlockResponseJson{
			Version: respContainer.Version,
			Data: &SignedBeaconBlockContainerJson{
				Message:   respContainer.Data.Phase0Block,
				Signature: respContainer.Data.Signature,
			},
			ExecutionOptimistic: respContainer.ExecutionOptimistic,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_ALTAIR.String())):
		actualRespContainer = &altairBlockResponseJson{
			Version: respContainer.Version,
			Data: &SignedBeaconBlockAltairContainerJson{
				Message:   respContainer.Data.AltairBlock,
				Signature: respContainer.Data.Signature,
			},
			ExecutionOptimistic: respContainer.ExecutionOptimistic,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_BELLATRIX.String())):
		actualRespContainer = &bellatrixBlockResponseJson{
			Version: respContainer.Version,
			Data: &SignedBeaconBlockBellatrixContainerJson{
				Message:   respContainer.Data.BellatrixBlock,
				Signature: respContainer.Data.Signature,
			},
			ExecutionOptimistic: respContainer.ExecutionOptimistic,
		}
	default:
		return false, nil, apimiddleware.InternalServerError(fmt.Errorf("unsupported block version '%s'", respContainer.Version))
	}

	j, err := json.Marshal(actualRespContainer)
	if err != nil {
		return false, nil, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal response")
	}
	return false, j, nil
}

type phase0StateResponseJson struct {
	Version string           `json:"version"`
	Data    *BeaconStateJson `json:"data"`
}

type altairStateResponseJson struct {
	Version string                 `json:"version"`
	Data    *BeaconStateAltairJson `json:"data"`
}

type bellatrixStateResponseJson struct {
	Version string                    `json:"version"`
	Data    *BeaconStateBellatrixJson `json:"data"`
}

func serializeV2State(response interface{}) (apimiddleware.RunDefault, []byte, apimiddleware.ErrorJson) {
	respContainer, ok := response.(*BeaconStateV2ResponseJson)
	if !ok {
		return false, nil, apimiddleware.InternalServerError(errors.New("container is not of the correct type"))
	}

	var actualRespContainer interface{}
	switch {
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_PHASE0.String())):
		actualRespContainer = &phase0StateResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.Phase0State,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_ALTAIR.String())):
		actualRespContainer = &altairStateResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.AltairState,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_BELLATRIX.String())):
		actualRespContainer = &bellatrixStateResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.BellatrixState,
		}
	default:
		return false, nil, apimiddleware.InternalServerError(fmt.Errorf("unsupported state version '%s'", respContainer.Version))
	}

	j, err := json.Marshal(actualRespContainer)
	if err != nil {
		return false, nil, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal response")
	}
	return false, j, nil
}

type phase0ProduceBlockResponseJson struct {
	Version string           `json:"version"`
	Data    *BeaconBlockJson `json:"data"`
}

type altairProduceBlockResponseJson struct {
	Version string                 `json:"version"`
	Data    *BeaconBlockAltairJson `json:"data"`
}

type bellatrixProduceBlockResponseJson struct {
	Version string                    `json:"version"`
	Data    *BeaconBlockBellatrixJson `json:"data"`
}

type bellatrixProduceBlindedBlockResponseJson struct {
	Version string                           `json:"version"`
	Data    *BlindedBeaconBlockBellatrixJson `json:"data"`
}

func serializeProducedV2Block(response interface{}) (apimiddleware.RunDefault, []byte, apimiddleware.ErrorJson) {
	respContainer, ok := response.(*ProduceBlockResponseV2Json)
	if !ok {
		return false, nil, apimiddleware.InternalServerError(errors.New("container is not of the correct type"))
	}

	var actualRespContainer interface{}
	switch {
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_PHASE0.String())):
		actualRespContainer = &phase0ProduceBlockResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.Phase0Block,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_ALTAIR.String())):
		actualRespContainer = &altairProduceBlockResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.AltairBlock,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_BELLATRIX.String())):
		actualRespContainer = &bellatrixProduceBlockResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.BellatrixBlock,
		}
	default:
		return false, nil, apimiddleware.InternalServerError(fmt.Errorf("unsupported block version '%s'", respContainer.Version))
	}

	j, err := json.Marshal(actualRespContainer)
	if err != nil {
		return false, nil, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal response")
	}
	return false, j, nil
}

func serializeProducedBlindedBlock(response interface{}) (apimiddleware.RunDefault, []byte, apimiddleware.ErrorJson) {
	respContainer, ok := response.(*ProduceBlindedBlockResponseJson)
	if !ok {
		return false, nil, apimiddleware.InternalServerError(errors.New("container is not of the correct type"))
	}

	var actualRespContainer interface{}
	switch {
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_PHASE0.String())):
		actualRespContainer = &phase0ProduceBlockResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.Phase0Block,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_ALTAIR.String())):
		actualRespContainer = &altairProduceBlockResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.AltairBlock,
		}
	case strings.EqualFold(respContainer.Version, strings.ToLower(ethpbv2.Version_BELLATRIX.String())):
		actualRespContainer = &bellatrixProduceBlindedBlockResponseJson{
			Version: respContainer.Version,
			Data:    respContainer.Data.BellatrixBlock,
		}
	default:
		return false, nil, apimiddleware.InternalServerError(fmt.Errorf("unsupported block version '%s'", respContainer.Version))
	}

	j, err := json.Marshal(actualRespContainer)
	if err != nil {
		return false, nil, apimiddleware.InternalServerErrorWithMessage(err, "could not marshal response")
	}
	return false, j, nil
}
