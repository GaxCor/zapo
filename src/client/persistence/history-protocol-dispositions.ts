import { setImmediate as yieldImmediate } from 'node:timers/promises'

import { isGroupJid, parseJidFull, splitJid } from '@protocol/jid'

type FieldRule = string | { readonly enum: readonly number[] } | { readonly packed: FieldRule }

/**
 * Closed rules pinned to the 1.8.2 WAProto source. This is structural validation,
 * not canonical support for every Message kind reachable through quote context.
 * No descriptors are loaded or reflected at runtime.
 */
const PINNED_RULES: Readonly<Record<string, Readonly<Record<number, FieldRule>>>> = {
    AIMediaCollectionMessage: { '1': 'string', '2': 'uint32', '3': 'bool' },
    AIMediaCollectionMetadata: { '1': 'string', '2': 'uint32' },
    AIMetadataOperation: { '1': 'HatchMetadataSync' },
    AIProvenance: { '1': 'AIProvenance.Metadata', '2': 'AIProvenance.Metadata' },
    'AIProvenance.Metadata': { '1': 'bool', '2': 'bool' },
    AIQueryFanout: { '1': 'MessageKey', '2': 'Message', '3': 'int64' },
    AIRegenerateMetadata: { '1': 'MessageKey', '2': 'int64' },
    AIRichResponseCodeMetadata: {
        '1': 'string',
        '2': 'AIRichResponseCodeMetadata.AIRichResponseCodeBlock'
    },
    'AIRichResponseCodeMetadata.AIRichResponseCodeBlock': {
        '1': { enum: [0, 1, 2, 3, 4, 5] },
        '2': 'string'
    },
    AIRichResponseContentItemsMetadata: {
        '1': 'AIRichResponseContentItemsMetadata.AIRichResponseContentItemMetadata',
        '2': { enum: [0, 1] }
    },
    'AIRichResponseContentItemsMetadata.AIRichResponseContentItemMetadata': {
        '1': 'AIRichResponseContentItemsMetadata.AIRichResponseReelItem'
    },
    'AIRichResponseContentItemsMetadata.AIRichResponseReelItem': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'string'
    },
    AIRichResponseDynamicMetadata: {
        '1': { enum: [0, 1, 2] },
        '2': 'uint64',
        '3': 'string',
        '4': 'uint32'
    },
    AIRichResponseGridImageMetadata: {
        '1': 'AIRichResponseImageURL',
        '2': 'AIRichResponseImageURL'
    },
    AIRichResponseImageURL: { '1': 'string', '2': 'string', '3': 'string' },
    AIRichResponseInlineImageMetadata: {
        '1': 'AIRichResponseImageURL',
        '2': 'string',
        '3': { enum: [0, 1, 2] },
        '4': 'string'
    },
    AIRichResponseLatexMetadata: {
        '1': 'string',
        '2': 'AIRichResponseLatexMetadata.AIRichResponseLatexExpression'
    },
    'AIRichResponseLatexMetadata.AIRichResponseLatexExpression': {
        '1': 'string',
        '2': 'string',
        '3': 'double',
        '4': 'double',
        '5': 'double',
        '6': 'double',
        '7': 'double',
        '8': 'double',
        '9': 'double'
    },
    AIRichResponseMapMetadata: {
        '1': 'double',
        '2': 'double',
        '3': 'double',
        '4': 'double',
        '5': 'AIRichResponseMapMetadata.AIRichResponseMapAnnotation',
        '6': 'bool'
    },
    'AIRichResponseMapMetadata.AIRichResponseMapAnnotation': {
        '1': 'uint32',
        '2': 'double',
        '3': 'double',
        '4': 'string',
        '5': 'string'
    },
    AIRichResponseMessage: {
        '1': { enum: [0, 1] },
        '2': 'AIRichResponseSubMessage',
        '3': 'AIRichResponseUnifiedResponse',
        '4': 'ContextInfo'
    },
    AIRichResponseSubMessage: {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] },
        '2': 'AIRichResponseGridImageMetadata',
        '3': 'string',
        '4': 'AIRichResponseInlineImageMetadata',
        '5': 'AIRichResponseCodeMetadata',
        '6': 'AIRichResponseTableMetadata',
        '7': 'AIRichResponseDynamicMetadata',
        '8': 'AIRichResponseLatexMetadata',
        '9': 'AIRichResponseMapMetadata',
        '10': 'AIRichResponseContentItemsMetadata'
    },
    AIRichResponseTableMetadata: {
        '1': 'AIRichResponseTableMetadata.AIRichResponseTableRow',
        '2': 'string'
    },
    'AIRichResponseTableMetadata.AIRichResponseTableRow': { '1': 'string', '2': 'bool' },
    AIRichResponseUnifiedResponse: { '1': 'bytes' },
    AISubscriptionUpsellMetadata: { '1': { enum: [0, 1, 2, 3] } },
    AIThreadInfo: {
        '1': 'AIThreadInfo.AIThreadServerInfo',
        '2': 'AIThreadInfo.AIThreadClientInfo'
    },
    'AIThreadInfo.AIThreadClientInfo': { '1': { enum: [0, 1, 2, 3] }, '2': 'string' },
    'AIThreadInfo.AIThreadServerInfo': { '1': 'string' },
    ActionLink: { '1': 'string', '2': 'string' },
    AutoDownloadSettings: { '1': 'bool', '2': 'bool', '3': 'bool', '4': 'bool' },
    AvatarUserSettings: { '1': 'string', '2': 'string' },
    BotAgeCollectionMetadata: { '1': 'bool', '2': 'bool', '3': { enum: [0, 1] } },
    BotAgentDeepLinkMetadata: { '1': 'string', '2': 'bytes' },
    BotAgentMetadata: { '1': 'BotAgentDeepLinkMetadata' },
    BotCapabilityMetadata: {
        '1': {
            packed: {
                enum: [
                    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
                    22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
                    42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61,
                    62, 63, 64, 65, 66
                ]
            }
        }
    },
    BotCommandMetadata: { '1': 'string', '2': 'string', '3': 'string' },
    BotDocumentMessageMetadata: { '1': { enum: [0, 1] } },
    BotFeedbackMessage: {
        '1': 'MessageKey',
        '2': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14] },
        '3': 'string',
        '4': 'uint64',
        '5': 'uint64',
        '6': { enum: [0, 1] },
        '7': 'BotFeedbackMessage.SideBySideSurveyMetadata'
    },
    'BotFeedbackMessage.SideBySideSurveyMetadata': {
        '1': 'string',
        '2': 'uint32',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'bool',
        '7': 'string',
        '8': 'BotFeedbackMessage.SideBySideSurveyMetadata.SideBySideSurveyAnalyticsData',
        '9': 'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData'
    },
    'BotFeedbackMessage.SideBySideSurveyMetadata.SideBySideSurveyAnalyticsData': {
        '1': 'string',
        '2': 'string',
        '3': 'string'
    },
    'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData': {
        '1': 'uint32',
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyCTAImpressionEventData',
        '6': 'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyCTAClickEventData',
        '7': 'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyCardImpressionEventData',
        '8': 'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyResponseEventData',
        '9': 'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyAbandonEventData'
    },
    'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyAbandonEventData':
        { '1': 'string' },
    'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyCTAClickEventData':
        { '1': 'bool', '2': 'string' },
    'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyCTAImpressionEventData':
        { '1': 'bool' },
    'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyCardImpressionEventData':
        {},
    'BotFeedbackMessage.SideBySideSurveyMetadata.SidebySideSurveyMetaAiAnalyticsData.SideBySideSurveyResponseEventData':
        { '1': 'string', '2': 'string' },
    BotGroupMetadata: { '1': 'BotGroupParticipantMetadata' },
    BotGroupParticipantMetadata: { '1': 'string' },
    BotHistoryShareMetadata: { '1': 'BotGroupParticipantMetadata' },
    BotImagineMetadata: { '1': { enum: [0, 1, 2, 3, 4] }, '2': 'string' },
    BotInfrastructureDiagnostics: { '1': { enum: [0, 1] }, '2': 'string', '3': 'bool' },
    BotLinkedAccount: { '1': { enum: [0] } },
    BotLinkedAccountsMetadata: { '1': 'BotLinkedAccount', '2': 'bytes', '3': 'int32' },
    BotMediaMetadata: {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'int64',
        '6': 'string',
        '7': { enum: [1, 2, 3] }
    },
    BotMemoryFact: { '1': 'string', '2': 'string' },
    BotMemoryMetadata: { '1': 'BotMemoryFact', '2': 'BotMemoryFact', '3': 'string' },
    BotMemuMetadata: { '1': 'BotMediaMetadata' },
    BotMessageOrigin: { '1': { enum: [0] } },
    BotMessageOriginMetadata: { '1': 'BotMessageOrigin' },
    BotMessageSharingInfo: {
        '1': {
            enum: [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 45, 46,
                47, 54, 55, 56
            ]
        },
        '2': 'uint32'
    },
    BotMetadata: {
        '2': 'string',
        '3': 'BotPluginMetadata',
        '4': 'BotSuggestedPromptMetadata',
        '5': 'string',
        '6': 'BotSessionMetadata',
        '7': 'BotMemuMetadata',
        '8': 'string',
        '9': 'BotReminderMetadata',
        '10': 'BotModelMetadata',
        '11': 'string',
        '12': 'BotProgressIndicatorMetadata',
        '13': 'BotCapabilityMetadata',
        '14': 'BotImagineMetadata',
        '15': 'BotMemoryMetadata',
        '16': 'BotRenderingMetadata',
        '17': 'BotMetricsMetadata',
        '18': 'BotLinkedAccountsMetadata',
        '19': 'BotSourcesMetadata',
        '20': 'bytes',
        '21': 'BotPromotionMessageMetadata',
        '22': 'BotModeSelectionMetadata',
        '23': 'BotQuotaMetadata',
        '24': 'BotAgeCollectionMetadata',
        '25': 'string',
        '26': 'string',
        '27': 'BotSignatureVerificationMetadata',
        '28': 'BotUnifiedResponseMutation',
        '29': 'BotMessageOriginMetadata',
        '30': 'InThreadSurveyMetadata',
        '31': 'AIThreadInfo',
        '32': 'AIRegenerateMetadata',
        '33': 'SessionTransparencyMetadata',
        '34': 'BotDocumentMessageMetadata',
        '35': 'BotGroupMetadata',
        '36': 'BotRenderingConfigMetadata',
        '37': 'BotInfrastructureDiagnostics',
        '38': 'AIMediaCollectionMetadata',
        '39': 'BotCommandMetadata',
        '40': 'BotResolvedToolCallMetadata',
        '41': 'AISubscriptionUpsellMetadata',
        '42': 'BotPttPromptMetadata',
        '43': 'BotHistoryShareMetadata',
        '999': 'bytes'
    },
    BotMetricsMetadata: {
        '1': 'string',
        '2': {
            enum: [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 45, 46,
                47, 54, 55, 56
            ]
        },
        '3': { enum: [1, 2, 3, 4, 5] }
    },
    BotModeSelectionMetadata: { '1': { packed: { enum: [0, 1] } }, '2': { packed: 'uint32' } },
    BotModelMetadata: { '1': { enum: [0, 1, 2] }, '2': { enum: [0, 1, 2] }, '3': 'string' },
    BotPluginMetadata: {
        '1': { enum: [0, 1, 2, 3] },
        '2': { enum: [0, 1, 2] },
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'uint32',
        '7': 'uint32',
        '9': 'string',
        '10': 'MessageKey',
        '11': { enum: [0, 1, 2] },
        '12': { enum: [0, 1, 2] },
        '13': 'string'
    },
    BotProgressIndicatorMetadata: {
        '1': 'string',
        '2': 'BotProgressIndicatorMetadata.BotPlanningStepMetadata',
        '3': 'int64'
    },
    'BotProgressIndicatorMetadata.BotPlanningStepMetadata': {
        '1': 'string',
        '2': 'string',
        '3': 'BotProgressIndicatorMetadata.BotPlanningStepMetadata.BotPlanningSearchSourcesMetadata',
        '4': { enum: [0, 1, 2, 3] },
        '5': 'bool',
        '6': 'bool',
        '7': 'BotProgressIndicatorMetadata.BotPlanningStepMetadata.BotPlanningStepSectionMetadata'
    },
    'BotProgressIndicatorMetadata.BotPlanningStepMetadata.BotPlanningSearchSourceMetadata': {
        '1': 'string',
        '2': { enum: [0, 1, 2, 3] },
        '3': 'string',
        '4': 'string'
    },
    'BotProgressIndicatorMetadata.BotPlanningStepMetadata.BotPlanningSearchSourcesMetadata': {
        '1': 'string',
        '2': { enum: [0, 1, 2, 3] },
        '3': 'string'
    },
    'BotProgressIndicatorMetadata.BotPlanningStepMetadata.BotPlanningStepSectionMetadata': {
        '1': 'string',
        '2': 'string',
        '3': 'BotProgressIndicatorMetadata.BotPlanningStepMetadata.BotPlanningSearchSourceMetadata'
    },
    BotPromotionMessageMetadata: { '1': { enum: [0, 1, 2] }, '2': 'string' },
    BotPromptSuggestion: { '1': 'string', '2': 'string' },
    BotPromptSuggestions: { '1': 'BotPromptSuggestion' },
    BotPttPromptMetadata: { '1': 'string' },
    BotQuotaMetadata: { '1': 'BotQuotaMetadata.BotFeatureQuotaMetadata' },
    'BotQuotaMetadata.BotFeatureQuotaMetadata': {
        '1': { enum: [0, 1] },
        '2': 'uint32',
        '3': 'uint64'
    },
    BotReminderMetadata: {
        '1': 'MessageKey',
        '2': { enum: [1, 2, 3, 4] },
        '3': 'string',
        '4': 'uint64',
        '5': { enum: [1, 2, 3, 4, 5] }
    },
    BotRenderingConfigMetadata: { '1': 'string', '2': 'double' },
    BotRenderingMetadata: { '1': 'BotRenderingMetadata.Keyword' },
    'BotRenderingMetadata.Keyword': { '1': 'string', '2': 'string' },
    BotResolvedToolCallMetadata: { '1': 'string', '2': 'string' },
    BotSessionMetadata: { '1': 'string', '2': { enum: [0, 1, 2, 3, 4, 5, 6, 7] } },
    BotSignatureVerificationMetadata: { '1': 'BotSignatureVerificationUseCaseProof' },
    BotSignatureVerificationUseCaseProof: {
        '1': 'int32',
        '2': { enum: [0, 1, 2, 3] },
        '3': 'bytes',
        '4': 'bytes'
    },
    BotSourcesMetadata: { '1': 'BotSourcesMetadata.BotSourceItem' },
    'BotSourcesMetadata.BotSourceItem': {
        '1': { enum: [0, 1, 2, 3, 4] },
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'uint32',
        '7': 'string'
    },
    BotSuggestedPromptMetadata: {
        '1': 'string',
        '2': 'uint32',
        '3': 'BotPromptSuggestions',
        '4': 'string'
    },
    BotUnifiedResponseMutation: {
        '1': 'BotUnifiedResponseMutation.SideBySideMetadata',
        '2': 'BotUnifiedResponseMutation.MediaDetailsMetadata'
    },
    'BotUnifiedResponseMutation.MediaDetailsMetadata': {
        '1': 'string',
        '2': 'BotMediaMetadata',
        '3': 'BotMediaMetadata'
    },
    'BotUnifiedResponseMutation.SideBySideMetadata': { '1': 'string', '2': 'bool' },
    CallLogRecord: {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
        '2': 'bool',
        '3': { enum: [0, 1, 2, 3] },
        '4': 'int64',
        '5': 'int64',
        '6': 'bool',
        '7': 'bool',
        '8': 'bool',
        '9': 'string',
        '10': 'string',
        '11': 'string',
        '12': 'string',
        '13': 'string',
        '14': 'CallLogRecord.ParticipantInfo',
        '15': { enum: [0, 1, 2] }
    },
    'CallLogRecord.ParticipantInfo': {
        '1': 'string',
        '2': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10] }
    },
    ChatLockSettings: { '1': 'bool', '2': 'UserPassword' },
    Citation: { '1': 'string', '2': 'string', '3': 'string', '4': 'string' },
    CoexStateSync: { '1': 'CoexStateSync.CollectionMutations' },
    'CoexStateSync.CollectionMutations': { '1': 'string', '2': 'CoexStateSync.Mutation' },
    'CoexStateSync.Mutation': {
        '1': 'SyncdIndex',
        '2': 'SyncdValue',
        '3': 'uint64',
        '4': { enum: [0, 1] }
    },
    CommentMetadata: { '1': 'MessageKey', '2': 'uint32' },
    ContextInfo: {
        '1': 'string',
        '2': 'string',
        '3': 'Message',
        '4': 'string',
        '15': 'string',
        '18': 'string',
        '19': 'bytes',
        '20': 'uint32',
        '21': 'uint32',
        '22': 'bool',
        '23': 'ContextInfo.AdReplyInfo',
        '24': 'MessageKey',
        '25': 'uint32',
        '26': 'int64',
        '27': 'bytes',
        '28': 'ContextInfo.ExternalAdReplyInfo',
        '29': 'string',
        '30': 'string',
        '31': 'uint32',
        '32': 'DisappearingMode',
        '33': 'ActionLink',
        '34': 'string',
        '35': 'string',
        '37': 'string',
        '38': 'uint32',
        '39': 'bool',
        '40': 'GroupMention',
        '41': 'ContextInfo.UTMInfo',
        '43': 'ContextInfo.ForwardedNewsletterMessageInfo',
        '44': 'ContextInfo.BusinessMessageForwardInfo',
        '45': 'string',
        '46': 'string',
        '47': 'ContextInfo.DataSharingContext',
        '48': 'bool',
        '49': 'ContextInfo.FeatureEligibilities',
        '50': 'string',
        '51': 'string',
        '54': 'string',
        '55': 'bytes',
        '56': 'ForwardedAIBotMessageInfo',
        '57': { enum: [0, 1, 2, 3, 4] },
        '58': 'UrlTrackingMap',
        '59': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8] },
        '60': 'uint32',
        '62': 'MemberLabel',
        '63': 'bool',
        '64': { enum: [0, 1, 2, 3, 4, 5] },
        '65': 'StatusAttribution',
        '66': 'bool',
        '67': { enum: [0, 1, 2, 3, 4, 5] },
        '68': 'ContextInfo.QuestionReplyQuotedMessage',
        '69': 'ContextInfo.StatusAudienceMetadata',
        '70': 'uint32',
        '71': { enum: [0, 1] },
        '72': 'BotMessageSharingInfo',
        '73': 'bool',
        '74': 'MediaDomainInfo',
        '75': 'ContextInfo.PartiallySelectedContent',
        '76': 'uint32',
        '77': { enum: [0, 1, 2] },
        '78': 'ContextInfo.BusinessInteractionPills',
        '79': 'string',
        '80': 'ContextInfo.InstagramThreadLink',
        '81': 'AIProvenance'
    },
    'ContextInfo.AdReplyInfo': {
        '1': 'string',
        '2': { enum: [0, 1, 2] },
        '16': 'bytes',
        '17': 'string'
    },
    'ContextInfo.BusinessInteractionPills': {
        '1': 'string',
        '2': 'ContextInfo.BusinessInteractionPills.Pill',
        '3': { enum: [0, 1, 2, 3, 4, 5] },
        '4': 'bytes',
        '5': 'BotSignatureVerificationMetadata',
        '6': 'ContextInfo.BusinessInteractionPills.UnauthenticatedBusinessMetadata'
    },
    'ContextInfo.BusinessInteractionPills.Pill': {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] },
        '2': 'string'
    },
    'ContextInfo.BusinessInteractionPills.UnauthenticatedBusinessMetadata': {
        '1': 'string',
        '2': 'string',
        '3': 'bool',
        '4': 'int64'
    },
    'ContextInfo.BusinessMessageForwardInfo': { '1': 'string' },
    'ContextInfo.DataSharingContext': {
        '1': 'bool',
        '2': 'string',
        '3': 'ContextInfo.DataSharingContext.Parameters',
        '4': 'int32'
    },
    'ContextInfo.DataSharingContext.Parameters': {
        '1': 'string',
        '2': 'string',
        '3': 'int64',
        '4': 'float',
        '5': 'ContextInfo.DataSharingContext.Parameters'
    },
    'ContextInfo.ExternalAdReplyInfo': {
        '1': 'string',
        '2': 'string',
        '3': { enum: [0, 1, 2] },
        '4': 'string',
        '5': 'string',
        '6': 'bytes',
        '7': 'string',
        '8': 'string',
        '9': 'string',
        '10': 'bool',
        '11': 'bool',
        '12': 'bool',
        '13': 'string',
        '14': 'string',
        '15': 'bool',
        '16': 'bool',
        '17': 'string',
        '18': 'bool',
        '19': 'string',
        '20': 'string',
        '21': 'bool',
        '22': 'string',
        '23': 'string',
        '24': 'bool',
        '25': { enum: [0, 1] },
        '26': 'string',
        '27': 'string',
        '28': 'bool',
        '29': 'int32',
        '30': 'int32',
        '31': 'int32',
        '32': 'int32'
    },
    'ContextInfo.FeatureEligibilities': {
        '1': 'bool',
        '2': 'bool',
        '3': 'bool',
        '4': 'bool',
        '5': 'bool'
    },
    'ContextInfo.ForwardedNewsletterMessageInfo': {
        '1': 'string',
        '2': 'int32',
        '3': 'string',
        '4': { enum: [1, 2, 3] },
        '5': 'string',
        '6': 'string'
    },
    'ContextInfo.InstagramThreadLink': { '1': 'string' },
    'ContextInfo.PartiallySelectedContent': { '1': 'string' },
    'ContextInfo.QuestionReplyQuotedMessage': { '1': 'int32', '2': 'Message', '3': 'Message' },
    'ContextInfo.StatusAudienceMetadata': { '1': { enum: [0, 1] }, '2': 'string', '3': 'string' },
    'ContextInfo.UTMInfo': { '1': 'string', '2': 'string' },
    DeviceListMetadata: {
        '1': 'bytes',
        '2': 'uint64',
        '3': { packed: 'uint32' },
        '4': { enum: [0, 1, 2] },
        '5': { enum: [0, 1, 2] },
        '8': 'bytes',
        '9': 'uint64',
        '10': { packed: 'uint32' }
    },
    'DeviceProps.HistorySyncConfig': {
        '1': 'uint32',
        '2': 'uint32',
        '3': 'uint32',
        '4': 'bool',
        '5': 'uint32',
        '6': 'bool',
        '7': 'bool',
        '8': 'bool',
        '9': 'bool',
        '10': 'bool',
        '11': 'bool',
        '12': 'bool',
        '13': 'bool',
        '14': 'bool',
        '15': 'bool',
        '16': 'bool',
        '17': 'bool',
        '18': 'bool',
        '19': 'uint32',
        '20': 'uint32',
        '21': 'bool',
        '22': 'bool',
        '23': 'string',
        '24': 'bool',
        '25': 'bool'
    },
    DisappearingMode: {
        '1': { enum: [0, 1, 2, 3] },
        '2': { enum: [0, 1, 2, 3, 4, 5] },
        '3': 'string',
        '4': 'bool'
    },
    EmbeddedContent: { '1': 'EmbeddedMessage', '2': 'EmbeddedMusic' },
    EmbeddedMessage: { '1': 'string', '2': 'Message' },
    EmbeddedMusic: {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'bytes',
        '7': 'bytes',
        '8': 'string',
        '9': 'bytes',
        '10': 'bool',
        '11': 'bytes',
        '12': 'int64',
        '13': 'int64',
        '14': 'int64'
    },
    EventAdditionalMetadata: { '1': 'bool' },
    EventResponse: {
        '1': 'MessageKey',
        '2': 'int64',
        '3': 'Message.EventResponseMessage',
        '4': 'bool'
    },
    ForwardedAIBotMessageInfo: { '1': 'string', '2': 'string', '3': 'string' },
    GlobalSettings: {
        '1': 'WallpaperSettings',
        '2': { enum: [0, 1, 2] },
        '3': 'WallpaperSettings',
        '4': 'AutoDownloadSettings',
        '5': 'AutoDownloadSettings',
        '6': 'AutoDownloadSettings',
        '7': 'bool',
        '8': 'bool',
        '9': 'int32',
        '10': 'int64',
        '11': 'AvatarUserSettings',
        '12': 'int32',
        '13': 'bool',
        '14': 'bool',
        '15': 'int32',
        '16': 'int32',
        '17': 'NotificationSettings',
        '18': 'NotificationSettings',
        '19': 'ChatLockSettings',
        '20': 'int64'
    },
    GroupHistoryBundleInfo: {
        '1': 'Message.MessageHistoryBundle',
        '2': { enum: [0, 1, 2, 3, 4, 5] }
    },
    GroupHistoryIndividualMessageInfo: { '1': 'MessageKey', '2': 'bool' },
    GroupMention: { '1': 'string', '2': 'string' },
    GroupRootKeyShare: { '1': 'GroupRootKeyShareEntry' },
    GroupRootKeyShareEntry: { '1': 'bytes', '2': 'string', '3': 'int64', '4': 'int64' },
    HatchMetadataSync: { '1': 'bytes', '2': 'int64', '3': 'string' },
    HistorySyncMsg: { '1': 'WebMessageInfo', '2': 'uint64' },
    HydratedTemplateButton: {
        '1': 'HydratedTemplateButton.HydratedQuickReplyButton',
        '2': 'HydratedTemplateButton.HydratedURLButton',
        '3': 'HydratedTemplateButton.HydratedCallButton',
        '4': 'uint32'
    },
    'HydratedTemplateButton.HydratedCallButton': { '1': 'string', '2': 'string' },
    'HydratedTemplateButton.HydratedQuickReplyButton': { '1': 'string', '2': 'string' },
    'HydratedTemplateButton.HydratedURLButton': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': { enum: [1, 2, 3] }
    },
    InThreadSurveyMetadata: {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'string',
        '7': 'string',
        '8': 'string',
        '9': 'string',
        '10': 'string',
        '11': 'string',
        '12': 'InThreadSurveyMetadata.InThreadSurveyQuestion',
        '13': 'string',
        '14': 'string',
        '15': 'string',
        '16': 'InThreadSurveyMetadata.InThreadSurveyPrivacyStatementPart',
        '17': 'string',
        '18': 'int32'
    },
    'InThreadSurveyMetadata.InThreadSurveyOption': { '1': 'string', '2': 'uint32', '3': 'string' },
    'InThreadSurveyMetadata.InThreadSurveyPrivacyStatementPart': { '1': 'string', '2': 'string' },
    'InThreadSurveyMetadata.InThreadSurveyQuestion': {
        '1': 'string',
        '2': 'string',
        '3': 'InThreadSurveyMetadata.InThreadSurveyOption'
    },
    InteractiveAnnotation: {
        '1': 'Point',
        '2': 'Location',
        '3': 'ContextInfo.ForwardedNewsletterMessageInfo',
        '4': 'bool',
        '5': 'EmbeddedContent',
        '6': 'bool',
        '7': 'TapLinkAction',
        '8': { enum: [1, 2, 3] }
    },
    InteractiveMessageAdditionalMetadata: { '1': 'bool' },
    KeepInChat: {
        '1': { enum: [0, 1, 2] },
        '2': 'int64',
        '3': 'MessageKey',
        '4': 'string',
        '5': 'int64',
        '6': 'int64'
    },
    LIDMigrationMappingSyncMessage: { '1': 'bytes' },
    LegacyMessage: { '1': 'Message.EventResponseMessage', '2': 'Message.PollVoteMessage' },
    LimitSharing: { '1': 'bool', '2': { enum: [0, 1, 2, 3] }, '3': 'int64', '4': 'bool' },
    Location: { '1': 'double', '2': 'double', '3': 'string' },
    MediaData: { '1': 'string' },
    MediaDomainInfo: { '1': { enum: [0, 1, 2] }, '2': 'bytes' },
    MediaNotifyMessage: { '1': 'string', '2': 'bytes', '3': 'uint64' },
    MemberLabel: { '1': 'string', '2': 'int64' },
    Message: {
        '1': 'string',
        '2': 'Message.SenderKeyDistributionMessage',
        '3': 'Message.ImageMessage',
        '4': 'Message.ContactMessage',
        '5': 'Message.LocationMessage',
        '6': 'Message.ExtendedTextMessage',
        '7': 'Message.DocumentMessage',
        '8': 'Message.AudioMessage',
        '9': 'Message.VideoMessage',
        '10': 'Message.Call',
        '11': 'Message.Chat',
        '12': 'Message.ProtocolMessage',
        '13': 'Message.ContactsArrayMessage',
        '14': 'Message.HighlyStructuredMessage',
        '15': 'Message.SenderKeyDistributionMessage',
        '16': 'Message.SendPaymentMessage',
        '18': 'Message.LiveLocationMessage',
        '22': 'Message.RequestPaymentMessage',
        '23': 'Message.DeclinePaymentRequestMessage',
        '24': 'Message.CancelPaymentRequestMessage',
        '25': 'Message.TemplateMessage',
        '26': 'Message.StickerMessage',
        '28': 'Message.GroupInviteMessage',
        '29': 'Message.TemplateButtonReplyMessage',
        '30': 'Message.ProductMessage',
        '31': 'Message.DeviceSentMessage',
        '35': 'MessageContextInfo',
        '36': 'Message.ListMessage',
        '37': 'Message.FutureProofMessage',
        '38': 'Message.OrderMessage',
        '39': 'Message.ListResponseMessage',
        '40': 'Message.FutureProofMessage',
        '41': 'Message.InvoiceMessage',
        '42': 'Message.ButtonsMessage',
        '43': 'Message.ButtonsResponseMessage',
        '44': 'Message.PaymentInviteMessage',
        '45': 'Message.InteractiveMessage',
        '46': 'Message.ReactionMessage',
        '47': 'Message.StickerSyncRMRMessage',
        '48': 'Message.InteractiveResponseMessage',
        '49': 'Message.PollCreationMessage',
        '50': 'Message.PollUpdateMessage',
        '51': 'Message.KeepInChatMessage',
        '53': 'Message.FutureProofMessage',
        '54': 'Message.RequestPhoneNumberMessage',
        '55': 'Message.FutureProofMessage',
        '56': 'Message.EncReactionMessage',
        '58': 'Message.FutureProofMessage',
        '59': 'Message.FutureProofMessage',
        '60': 'Message.PollCreationMessage',
        '61': 'Message.ScheduledCallCreationMessage',
        '62': 'Message.FutureProofMessage',
        '63': 'Message.PinInChatMessage',
        '64': 'Message.PollCreationMessage',
        '65': 'Message.ScheduledCallEditMessage',
        '66': 'Message.VideoMessage',
        '67': 'Message.FutureProofMessage',
        '69': 'Message.CallLogMessage',
        '70': 'Message.MessageHistoryBundle',
        '71': 'Message.EncCommentMessage',
        '72': 'Message.BCallMessage',
        '74': 'Message.FutureProofMessage',
        '75': 'Message.EventMessage',
        '76': 'Message.EncEventResponseMessage',
        '77': 'Message.CommentMessage',
        '78': 'Message.NewsletterAdminInviteMessage',
        '80': 'Message.PlaceholderMessage',
        '82': 'Message.SecretEncryptedMessage',
        '83': 'Message.AlbumMessage',
        '85': 'Message.FutureProofMessage',
        '86': 'Message.StickerPackMessage',
        '87': 'Message.FutureProofMessage',
        '88': 'Message.PollResultSnapshotMessage',
        '90': 'Message.FutureProofMessage',
        '91': 'Message.FutureProofMessage',
        '92': 'Message.FutureProofMessage',
        '93': 'Message.FutureProofMessage',
        '95': 'Message.FutureProofMessage',
        '96': 'Message.FutureProofMessage',
        '97': 'AIRichResponseMessage',
        '98': 'Message.StatusNotificationMessage',
        '99': 'Message.FutureProofMessage',
        '100': 'Message.FutureProofMessage',
        '101': 'Message.FutureProofMessage',
        '102': 'Message.MessageHistoryNotice',
        '103': 'Message.FutureProofMessage',
        '104': 'Message.FutureProofMessage',
        '105': 'Message.StatusQuestionAnswerMessage',
        '106': 'Message.FutureProofMessage',
        '107': 'Message.QuestionResponseMessage',
        '109': 'Message.StatusQuotedMessage',
        '110': 'Message.StatusStickerInteractionMessage',
        '111': 'Message.PollCreationMessage',
        '113': 'Message.NewsletterFollowerInviteMessage',
        '115': 'Message.PollResultSnapshotMessage',
        '116': 'Message.FutureProofMessage',
        '117': 'Message.FutureProofMessage',
        '118': 'Message.FutureProofMessage',
        '119': 'Message.PollCreationMessage',
        '120': 'Message.ConditionalRevealMessage',
        '121': 'Message.PollAddOptionMessage',
        '122': 'Message.EventInviteMessage',
        '123': 'GroupRootKeyShare',
        '124': 'Message.PaymentReminderMessage',
        '125': 'Message.SplitPaymentMessage',
        '126': 'Message.FutureProofMessage',
        '127': 'Message.RootSecretDistributeMessage',
        '128': 'Message.SplitPaymentUpdateMessage'
    },
    'Message.AlbumMessage': { '2': 'uint32', '3': 'uint32', '17': 'ContextInfo' },
    'Message.AppStateFatalExceptionNotification': { '1': 'string', '2': 'int64' },
    'Message.AppStateSyncKey': {
        '1': 'Message.AppStateSyncKeyId',
        '2': 'Message.AppStateSyncKeyData'
    },
    'Message.AppStateSyncKeyData': {
        '1': 'bytes',
        '2': 'Message.AppStateSyncKeyFingerprint',
        '3': 'int64'
    },
    'Message.AppStateSyncKeyFingerprint': {
        '1': 'uint32',
        '2': 'uint32',
        '3': { packed: 'uint32' }
    },
    'Message.AppStateSyncKeyId': { '1': 'bytes' },
    'Message.AppStateSyncKeyRequest': { '1': 'Message.AppStateSyncKeyId' },
    'Message.AppStateSyncKeyShare': { '1': 'Message.AppStateSyncKey' },
    'Message.AudioMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'bytes',
        '4': 'uint64',
        '5': 'uint32',
        '6': 'bool',
        '7': 'bytes',
        '8': 'bytes',
        '9': 'string',
        '10': 'int64',
        '17': 'ContextInfo',
        '18': 'bytes',
        '19': 'bytes',
        '20': 'fixed32',
        '21': 'bool',
        '22': 'string'
    },
    'Message.BCallMessage': {
        '1': 'string',
        '2': { enum: [0, 1, 2] },
        '3': 'bytes',
        '4': 'string'
    },
    'Message.BotHistoryShareSyncMetadata': {
        '1': 'string',
        '2': 'int64',
        '3': 'Message.HistoryShareMessageEntry'
    },
    'Message.ButtonsMessage': {
        '1': 'string',
        '2': 'Message.DocumentMessage',
        '3': 'Message.ImageMessage',
        '4': 'Message.VideoMessage',
        '5': 'Message.LocationMessage',
        '6': 'string',
        '7': 'string',
        '8': 'ContextInfo',
        '9': 'Message.ButtonsMessage.Button',
        '10': { enum: [0, 1, 2, 3, 4, 5, 6] }
    },
    'Message.ButtonsMessage.Button': {
        '1': 'string',
        '2': 'Message.ButtonsMessage.Button.ButtonText',
        '3': { enum: [0, 1, 2] },
        '4': 'Message.ButtonsMessage.Button.NativeFlowInfo'
    },
    'Message.ButtonsMessage.Button.ButtonText': { '1': 'string' },
    'Message.ButtonsMessage.Button.NativeFlowInfo': { '1': 'string', '2': 'string' },
    'Message.ButtonsResponseMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'ContextInfo',
        '4': { enum: [0, 1] }
    },
    'Message.Call': {
        '1': 'bytes',
        '2': 'string',
        '3': 'bytes',
        '4': 'uint32',
        '5': 'string',
        '6': 'bytes',
        '7': 'ContextInfo',
        '8': 'string',
        '9': 'string',
        '10': 'MessageContextInfo',
        '11': 'uint32'
    },
    'Message.CallLogMessage': {
        '1': 'bool',
        '2': { enum: [0, 1, 2, 3, 4, 5, 6, 7] },
        '3': 'int64',
        '4': { enum: [0, 1, 2] },
        '5': 'Message.CallLogMessage.CallParticipant'
    },
    'Message.CallLogMessage.CallParticipant': {
        '1': 'string',
        '2': { enum: [0, 1, 2, 3, 4, 5, 6, 7] }
    },
    'Message.CancelPaymentRequestMessage': { '1': 'MessageKey' },
    'Message.Chat': { '1': 'string', '2': 'string' },
    'Message.ChatCustomImageWallpaper': {
        '1': 'string',
        '2': 'bytes',
        '3': 'bytes',
        '4': 'bytes',
        '5': 'float'
    },
    'Message.ChatDefaultWallpaper': { '1': 'bool' },
    'Message.ChatSolidColorWallpaper': { '1': 'string', '2': 'string', '3': 'bool' },
    'Message.ChatStockImageWallpaper': { '1': 'string', '2': 'float' },
    'Message.ChatThemeSetting': {
        '1': 'int64',
        '2': 'bool',
        '3': 'string',
        '10': 'Message.ChatDefaultWallpaper',
        '11': 'Message.ChatSolidColorWallpaper',
        '12': 'Message.ChatStockImageWallpaper',
        '13': 'Message.ChatCustomImageWallpaper'
    },
    'Message.CloudAPIThreadControlNotification': {
        '1': { enum: [0, 1, 2, 3] },
        '2': 'int64',
        '3': 'string',
        '4': 'string',
        '5': 'Message.CloudAPIThreadControlNotification.CloudAPIThreadControlNotificationContent',
        '6': 'bool'
    },
    'Message.CloudAPIThreadControlNotification.CloudAPIThreadControlNotificationContent': {
        '1': 'string',
        '2': 'string'
    },
    'Message.CommentMessage': { '1': 'Message', '2': 'MessageKey' },
    'Message.ConditionalRevealMessage': {
        '1': 'bytes',
        '2': 'bytes',
        '3': { enum: [0, 1] },
        '4': 'string'
    },
    'Message.ContactMessage': { '1': 'string', '16': 'string', '17': 'ContextInfo', '18': 'bool' },
    'Message.ContactsArrayMessage': {
        '1': 'string',
        '2': 'Message.ContactMessage',
        '17': 'ContextInfo'
    },
    'Message.DeclinePaymentRequestMessage': { '1': 'MessageKey' },
    'Message.DeviceSentMessage': { '1': 'string', '2': 'Message', '3': 'string' },
    'Message.DocumentMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'bytes',
        '5': 'uint64',
        '6': 'uint32',
        '7': 'bytes',
        '8': 'string',
        '9': 'bytes',
        '10': 'string',
        '11': 'int64',
        '12': 'bool',
        '13': 'string',
        '14': 'bytes',
        '15': 'bytes',
        '16': 'bytes',
        '17': 'ContextInfo',
        '18': 'uint32',
        '19': 'uint32',
        '20': 'string',
        '21': 'string'
    },
    'Message.EncCommentMessage': { '1': 'MessageKey', '2': 'bytes', '3': 'bytes' },
    'Message.EncEventResponseMessage': { '1': 'MessageKey', '2': 'bytes', '3': 'bytes' },
    'Message.EncReactionMessage': { '1': 'MessageKey', '2': 'bytes', '3': 'bytes' },
    'Message.EventInviteMessage': {
        '1': 'ContextInfo',
        '2': 'string',
        '3': 'string',
        '4': 'bytes',
        '5': 'int64',
        '6': 'string',
        '7': 'bool',
        '8': 'int64',
        '9': 'string'
    },
    'Message.EventMessage': {
        '1': 'ContextInfo',
        '2': 'bool',
        '3': 'string',
        '4': 'string',
        '5': 'Message.LocationMessage',
        '6': 'string',
        '7': 'int64',
        '8': 'int64',
        '9': 'bool',
        '10': 'bool',
        '11': 'bool',
        '12': 'int64'
    },
    'Message.EventResponseMessage': { '1': { enum: [0, 1, 2, 3] }, '2': 'int64', '3': 'int32' },
    'Message.ExtendedTextMessage': {
        '1': 'string',
        '2': 'string',
        '4': 'excludedCanonicalUrl',
        '5': 'string',
        '6': 'string',
        '7': 'fixed32',
        '8': 'fixed32',
        '9': { enum: [0, 1, 2, 6, 7, 8, 9, 10] },
        '10': { enum: [0, 1, 4, 5, 6, 7] },
        '16': 'bytes',
        '17': 'ContextInfo',
        '18': 'bool',
        '19': 'string',
        '20': 'bytes',
        '21': 'bytes',
        '22': 'bytes',
        '23': 'int64',
        '24': 'uint32',
        '25': 'uint32',
        '26': { enum: [0, 1, 2, 3] },
        '27': 'string',
        '28': 'bytes',
        '29': { enum: [0, 1, 2, 3] },
        '30': 'bool',
        '31': 'uint32',
        '32': 'uint32',
        '33': 'Message.MMSThumbnailMetadata',
        '34': 'Message.LinkPreviewMetadata',
        '35': 'Message.PaymentLinkMetadata',
        '36': 'Message.VideoEndCard',
        '37': 'string',
        '38': 'EmbeddedMusic',
        '39': 'Message.PaymentExtendedMetadata'
    },
    'Message.FullHistorySyncOnDemandConfig': { '1': 'uint64', '2': 'uint32' },
    'Message.FullHistorySyncOnDemandRequestMetadata': {
        '1': 'string',
        '2': 'string',
        '3': 'bytes'
    },
    'Message.FutureProofMessage': { '1': 'Message' },
    'Message.GroupInviteMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'int64',
        '4': 'string',
        '5': 'bytes',
        '6': 'string',
        '7': 'ContextInfo',
        '8': { enum: [0, 1] }
    },
    'Message.HighlyStructuredMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'Message.HighlyStructuredMessage.HSMLocalizableParameter',
        '7': 'string',
        '8': 'string',
        '9': 'Message.TemplateMessage'
    },
    'Message.HighlyStructuredMessage.HSMLocalizableParameter': {
        '1': 'string',
        '2': 'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMCurrency',
        '3': 'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMDateTime'
    },
    'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMCurrency': {
        '1': 'string',
        '2': 'int64'
    },
    'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMDateTime': {
        '1': 'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMDateTime.HSMDateTimeComponent',
        '2': 'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMDateTime.HSMDateTimeUnixEpoch'
    },
    'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMDateTime.HSMDateTimeComponent': {
        '1': { enum: [1, 2, 3, 4, 5, 6, 7] },
        '2': 'uint32',
        '3': 'uint32',
        '4': 'uint32',
        '5': 'uint32',
        '6': 'uint32',
        '7': { enum: [1, 2] }
    },
    'Message.HighlyStructuredMessage.HSMLocalizableParameter.HSMDateTime.HSMDateTimeUnixEpoch': {
        '1': 'int64'
    },
    'Message.HistoryShareMessageEntry': { '1': 'string', '2': 'bytes' },
    'Message.HistorySyncMessageAccessStatus': { '1': 'bool' },
    'Message.HistorySyncNotification': {
        '1': 'bytes',
        '2': 'uint64',
        '3': 'bytes',
        '4': 'bytes',
        '5': 'string',
        '6': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8] },
        '7': 'uint32',
        '8': 'string',
        '9': 'uint32',
        '10': 'int64',
        '11': 'bytes',
        '12': 'string',
        '13': 'Message.FullHistorySyncOnDemandRequestMetadata',
        '14': 'string',
        '15': 'Message.HistorySyncMessageAccessStatus'
    },
    'Message.ImageMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'bytes',
        '5': 'uint64',
        '6': 'uint32',
        '7': 'uint32',
        '8': 'bytes',
        '9': 'bytes',
        '10': 'InteractiveAnnotation',
        '11': 'string',
        '12': 'int64',
        '16': 'bytes',
        '17': 'ContextInfo',
        '18': 'bytes',
        '19': 'uint32',
        '20': 'uint32',
        '21': 'bytes',
        '22': { packed: 'uint32' },
        '23': 'bytes',
        '24': 'bytes',
        '25': 'bool',
        '26': 'string',
        '27': 'bytes',
        '28': 'bytes',
        '29': 'string',
        '30': 'InteractiveAnnotation',
        '31': { enum: [0, 1, 2, 3] },
        '32': 'string',
        '34': 'string'
    },
    'Message.InitialSecurityNotificationSettingSync': { '1': 'bool' },
    'Message.InteractiveMessage': {
        '1': 'Message.InteractiveMessage.Header',
        '2': 'Message.InteractiveMessage.Body',
        '3': 'Message.InteractiveMessage.Footer',
        '4': 'Message.InteractiveMessage.ShopMessage',
        '5': 'Message.InteractiveMessage.CollectionMessage',
        '6': 'Message.InteractiveMessage.NativeFlowMessage',
        '7': 'Message.InteractiveMessage.CarouselMessage',
        '8': 'Message.InteractiveMessage.BloksWidget',
        '15': 'ContextInfo',
        '16': 'UrlTrackingMap'
    },
    'Message.InteractiveMessage.BloksWidget': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'string'
    },
    'Message.InteractiveMessage.Body': { '1': 'string' },
    'Message.InteractiveMessage.CarouselMessage': {
        '1': 'Message.InteractiveMessage',
        '2': 'int32',
        '3': { enum: [0, 1, 2] }
    },
    'Message.InteractiveMessage.CollectionMessage': { '1': 'string', '2': 'string', '3': 'int32' },
    'Message.InteractiveMessage.Footer': {
        '1': 'string',
        '2': 'Message.AudioMessage',
        '3': 'bool'
    },
    'Message.InteractiveMessage.Header': {
        '1': 'string',
        '2': 'string',
        '3': 'Message.DocumentMessage',
        '4': 'Message.ImageMessage',
        '5': 'bool',
        '6': 'bytes',
        '7': 'Message.VideoMessage',
        '8': 'Message.LocationMessage',
        '9': 'Message.ProductMessage',
        '10': 'Message.InteractiveMessage.BloksWidget'
    },
    'Message.InteractiveMessage.NativeFlowMessage': {
        '1': 'Message.InteractiveMessage.NativeFlowMessage.NativeFlowButton',
        '2': 'string',
        '3': 'int32'
    },
    'Message.InteractiveMessage.NativeFlowMessage.NativeFlowButton': {
        '1': 'string',
        '2': 'string'
    },
    'Message.InteractiveMessage.ShopMessage': {
        '1': 'string',
        '2': { enum: [0, 1, 2, 3] },
        '3': 'int32'
    },
    'Message.InteractiveResponseMessage': {
        '1': 'Message.InteractiveResponseMessage.Body',
        '2': 'Message.InteractiveResponseMessage.NativeFlowResponseMessage',
        '15': 'ContextInfo'
    },
    'Message.InteractiveResponseMessage.Body': { '1': 'string', '2': { enum: [0, 1] } },
    'Message.InteractiveResponseMessage.NativeFlowResponseMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'int32'
    },
    'Message.InvoiceMessage': {
        '1': 'string',
        '2': 'string',
        '3': { enum: [0, 1] },
        '4': 'string',
        '5': 'bytes',
        '6': 'int64',
        '7': 'bytes',
        '8': 'bytes',
        '9': 'string',
        '10': 'bytes'
    },
    'Message.KeepInChatMessage': { '1': 'MessageKey', '2': { enum: [0, 1, 2] }, '3': 'int64' },
    'Message.LinkPreviewMetadata': {
        '1': 'Message.PaymentLinkMetadata',
        '2': 'Message.URLMetadata',
        '3': 'uint32',
        '4': 'uint32',
        '5': { enum: [0, 1, 2, 3, 4, 5] },
        '6': 'bool',
        '7': 'string',
        '8': 'EmbeddedMusic',
        '9': 'string'
    },
    'Message.ListMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': { enum: [0, 1, 2] },
        '5': 'Message.ListMessage.Section',
        '6': 'Message.ListMessage.ProductListInfo',
        '7': 'string',
        '8': 'ContextInfo'
    },
    'Message.ListMessage.Product': { '1': 'string' },
    'Message.ListMessage.ProductListHeaderImage': { '1': 'string', '2': 'bytes' },
    'Message.ListMessage.ProductListInfo': {
        '1': 'Message.ListMessage.ProductSection',
        '2': 'Message.ListMessage.ProductListHeaderImage',
        '3': 'string'
    },
    'Message.ListMessage.ProductSection': { '1': 'string', '2': 'Message.ListMessage.Product' },
    'Message.ListMessage.Row': { '1': 'string', '2': 'string', '3': 'string' },
    'Message.ListMessage.Section': { '1': 'string', '2': 'Message.ListMessage.Row' },
    'Message.ListResponseMessage': {
        '1': 'string',
        '2': { enum: [0, 1] },
        '3': 'Message.ListResponseMessage.SingleSelectReply',
        '4': 'ContextInfo',
        '5': 'string'
    },
    'Message.ListResponseMessage.SingleSelectReply': { '1': 'string' },
    'Message.LiveLocationMessage': {
        '1': 'double',
        '2': 'double',
        '3': 'uint32',
        '4': 'float',
        '5': 'uint32',
        '6': 'string',
        '7': 'int64',
        '8': 'uint32',
        '16': 'bytes',
        '17': 'ContextInfo'
    },
    'Message.LocationMessage': {
        '1': 'double',
        '2': 'double',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'bool',
        '7': 'uint32',
        '8': 'float',
        '9': 'uint32',
        '11': 'string',
        '16': 'bytes',
        '17': 'ContextInfo'
    },
    'Message.MMSThumbnailMetadata': {
        '1': 'string',
        '2': 'bytes',
        '3': 'bytes',
        '4': 'bytes',
        '5': 'int64',
        '6': 'uint32',
        '7': 'uint32'
    },
    'Message.MarkAsVerifiedAction': { '1': 'string', '2': 'bool', '3': 'bytes', '4': 'uint64' },
    'Message.MessageHistoryBundle': {
        '1': 'string',
        '2': 'bytes',
        '3': 'bytes',
        '4': 'bytes',
        '5': 'string',
        '6': 'int64',
        '7': 'ContextInfo',
        '8': 'Message.MessageHistoryMetadata'
    },
    'Message.MessageHistoryMetadata': {
        '1': 'string',
        '2': 'int64',
        '3': 'int64',
        '4': 'string',
        '5': 'int64'
    },
    'Message.MessageHistoryNotice': {
        '1': 'ContextInfo',
        '2': 'Message.MessageHistoryMetadata',
        '3': 'Message.BotHistoryShareSyncMetadata'
    },
    'Message.NewsletterAdminInviteMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'bytes',
        '4': 'string',
        '5': 'int64',
        '6': 'ContextInfo'
    },
    'Message.NewsletterFollowerInviteMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'bytes',
        '4': 'string',
        '5': 'ContextInfo'
    },
    'Message.OrderMessage': {
        '1': 'string',
        '2': 'bytes',
        '3': 'int32',
        '4': { enum: [1, 2, 3] },
        '5': { enum: [1] },
        '6': 'string',
        '7': 'string',
        '8': 'string',
        '9': 'string',
        '10': 'int64',
        '11': 'string',
        '12': 'int32',
        '13': 'MessageKey',
        '15': 'string',
        '17': 'ContextInfo'
    },
    'Message.PaymentExtendedMetadata': { '1': 'uint32', '2': 'string', '3': 'string' },
    'Message.PaymentInviteMessage': {
        '1': { enum: [0, 1, 2, 3] },
        '2': 'int64',
        '3': 'bool',
        '4': 'string',
        '5': { enum: [0, 1] }
    },
    'Message.PaymentLinkMetadata': {
        '1': 'Message.PaymentLinkMetadata.PaymentLinkButton',
        '2': 'Message.PaymentLinkMetadata.PaymentLinkHeader',
        '3': 'Message.PaymentLinkMetadata.PaymentLinkProvider'
    },
    'Message.PaymentLinkMetadata.PaymentLinkButton': { '1': 'string' },
    'Message.PaymentLinkMetadata.PaymentLinkHeader': { '1': { enum: [0, 1] } },
    'Message.PaymentLinkMetadata.PaymentLinkProvider': { '1': 'string' },
    'Message.PaymentReminderMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': { enum: [0, 1, 2, 3, 4] },
        '5': { enum: [0, 1, 2, 3, 4, 5] },
        '6': 'string',
        '7': 'string',
        '8': 'string',
        '9': 'Money'
    },
    'Message.PeerDataOperationRequestMessage': {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] },
        '2': 'Message.PeerDataOperationRequestMessage.RequestStickerReupload',
        '3': 'Message.PeerDataOperationRequestMessage.RequestUrlPreview',
        '4': 'Message.PeerDataOperationRequestMessage.HistorySyncOnDemandRequest',
        '5': 'Message.PeerDataOperationRequestMessage.PlaceholderMessageResendRequest',
        '6': 'Message.PeerDataOperationRequestMessage.FullHistorySyncOnDemandRequest',
        '7': 'Message.PeerDataOperationRequestMessage.SyncDCollectionFatalRecoveryRequest',
        '8': 'Message.PeerDataOperationRequestMessage.HistorySyncChunkRetryRequest',
        '9': 'Message.PeerDataOperationRequestMessage.GalaxyFlowAction',
        '10': 'Message.PeerDataOperationRequestMessage.CompanionCanonicalUserNonceFetchRequest',
        '11': 'Message.PeerDataOperationRequestMessage.BizBroadcastInsightsContactListRequest',
        '12': 'Message.PeerDataOperationRequestMessage.BizBroadcastInsightsRefreshRequest'
    },
    'Message.PeerDataOperationRequestMessage.BizBroadcastInsightsContactListRequest': {
        '1': 'string'
    },
    'Message.PeerDataOperationRequestMessage.BizBroadcastInsightsRefreshRequest': { '1': 'string' },
    'Message.PeerDataOperationRequestMessage.CompanionCanonicalUserNonceFetchRequest': {
        '1': 'string'
    },
    'Message.PeerDataOperationRequestMessage.FullHistorySyncOnDemandRequest': {
        '1': 'Message.FullHistorySyncOnDemandRequestMetadata',
        '2': 'DeviceProps.HistorySyncConfig',
        '3': 'Message.FullHistorySyncOnDemandConfig'
    },
    'Message.PeerDataOperationRequestMessage.GalaxyFlowAction': {
        '1': { enum: [1, 2] },
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'string'
    },
    'Message.PeerDataOperationRequestMessage.HistorySyncChunkRetryRequest': {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8] },
        '2': 'uint32',
        '3': 'string',
        '4': 'bool'
    },
    'Message.PeerDataOperationRequestMessage.HistorySyncOnDemandRequest': {
        '1': 'string',
        '2': 'string',
        '3': 'bool',
        '4': 'int32',
        '5': 'int64',
        '6': 'string',
        '7': 'bool'
    },
    'Message.PeerDataOperationRequestMessage.PlaceholderMessageResendRequest': {
        '1': 'MessageKey'
    },
    'Message.PeerDataOperationRequestMessage.RequestStickerReupload': { '1': 'string' },
    'Message.PeerDataOperationRequestMessage.RequestUrlPreview': { '1': 'string', '2': 'bool' },
    'Message.PeerDataOperationRequestMessage.SyncDCollectionFatalRecoveryRequest': {
        '1': 'string',
        '2': 'int64'
    },
    'Message.PeerDataOperationRequestResponseMessage': {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] },
        '2': 'string',
        '3': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult'
    },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult': {
        '1': { enum: [0, 1, 2, 3] },
        '2': 'Message.StickerMessage',
        '3': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.LinkPreviewResponse',
        '4': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.PlaceholderMessageResendResponse',
        '5': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.WaffleNonceFetchResponse',
        '6': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.FullHistorySyncOnDemandRequestResponse',
        '7': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.CompanionMetaNonceFetchResponse',
        '8': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.SyncDSnapshotFatalRecoveryResponse',
        '9': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.CompanionCanonicalUserNonceFetchResponse',
        '10': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.HistorySyncChunkRetryResponse',
        '11': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.FlowResponsesCsvBundle',
        '12': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.BizBroadcastInsightsContactListResponse'
    },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.BizBroadcastInsightsContactListResponse':
        {
            '1': 'string',
            '2': 'int64',
            '3': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.BizBroadcastInsightsContactState'
        },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.BizBroadcastInsightsContactState':
        { '1': 'string', '2': { enum: [0, 1, 2, 3, 4] } },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.CompanionCanonicalUserNonceFetchResponse':
        { '1': 'string', '2': 'string', '3': 'bool' },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.CompanionMetaNonceFetchResponse':
        { '1': 'string' },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.FlowResponsesCsvBundle':
        {
            '1': 'string',
            '2': 'string',
            '3': 'string',
            '4': 'string',
            '5': 'bytes',
            '6': 'bytes',
            '7': 'bytes',
            '8': 'string',
            '9': 'int64',
            '10': 'uint64'
        },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.FullHistorySyncOnDemandRequestResponse':
        {
            '1': 'Message.FullHistorySyncOnDemandRequestMetadata',
            '2': { enum: [0, 1, 2, 3, 4, 5, 6, 7] }
        },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.HistorySyncChunkRetryResponse':
        {
            '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8] },
            '2': 'uint32',
            '3': 'string',
            '4': { enum: [1, 2, 3, 4, 5, 6] },
            '5': 'bool'
        },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.LinkPreviewResponse': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'bytes',
        '6': 'string',
        '7': 'string',
        '8': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.LinkPreviewResponse.LinkPreviewHighQualityThumbnail',
        '9': 'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.LinkPreviewResponse.PaymentLinkPreviewMetadata'
    },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.LinkPreviewResponse.LinkPreviewHighQualityThumbnail':
        {
            '1': 'string',
            '2': 'string',
            '3': 'string',
            '4': 'bytes',
            '5': 'int64',
            '6': 'int32',
            '7': 'int32'
        },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.LinkPreviewResponse.PaymentLinkPreviewMetadata':
        { '1': 'bool', '2': 'string', '3': 'string', '4': 'string', '5': 'string' },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.PlaceholderMessageResendResponse':
        { '1': 'bytes' },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.SyncDSnapshotFatalRecoveryResponse':
        { '1': 'bytes', '2': 'bool' },
    'Message.PeerDataOperationRequestResponseMessage.PeerDataOperationResult.WaffleNonceFetchResponse':
        { '1': 'string', '2': 'string' },
    'Message.PinInChatMessage': { '1': 'MessageKey', '2': { enum: [0, 1, 2] }, '3': 'int64' },
    'Message.PlaceholderMessage': { '1': { enum: [0] } },
    'Message.PollAddOptionMessage': {
        '1': 'MessageKey',
        '2': 'Message.PollCreationMessage.Option',
        '3': 'Message.PollUpdateMessageMetadata'
    },
    'Message.PollCreationMessage': {
        '1': 'bytes',
        '2': 'string',
        '3': 'Message.PollCreationMessage.Option',
        '4': 'uint32',
        '5': 'ContextInfo',
        '6': { enum: [0, 1, 2] },
        '7': { enum: [0, 1] },
        '8': 'Message.PollCreationMessage.Option',
        '9': 'int64',
        '10': 'bool',
        '11': 'bool'
    },
    'Message.PollCreationMessage.Option': { '1': 'string', '2': 'string' },
    'Message.PollEncValue': { '1': 'bytes', '2': 'bytes' },
    'Message.PollResultSnapshotMessage': {
        '1': 'string',
        '2': 'Message.PollResultSnapshotMessage.PollVote',
        '3': 'ContextInfo',
        '4': { enum: [0, 1] }
    },
    'Message.PollResultSnapshotMessage.PollVote': { '1': 'string', '2': 'int64' },
    'Message.PollUpdateMessage': {
        '1': 'MessageKey',
        '2': 'Message.PollEncValue',
        '3': 'Message.PollUpdateMessageMetadata',
        '4': 'int64'
    },
    'Message.PollUpdateMessageMetadata': { '1': 'bytes', '2': 'string' },
    'Message.PollVoteMessage': { '1': 'bytes' },
    'Message.ProductMessage': {
        '1': 'Message.ProductMessage.ProductSnapshot',
        '2': 'string',
        '4': 'Message.ProductMessage.CatalogSnapshot',
        '5': 'string',
        '6': 'string',
        '17': 'ContextInfo'
    },
    'Message.ProductMessage.CatalogSnapshot': {
        '1': 'Message.ImageMessage',
        '2': 'string',
        '3': 'string'
    },
    'Message.ProductMessage.ProductSnapshot': {
        '1': 'Message.ImageMessage',
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'int64',
        '7': 'string',
        '8': 'string',
        '9': 'uint32',
        '11': 'string',
        '12': 'int64',
        '13': 'string'
    },
    'Message.ProtocolMessage': {
        '1': 'MessageKey',
        '2': {
            enum: [
                0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 14, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
                28, 29, 30, 31, 32, 34, 35, 36, 37
            ]
        },
        '4': 'uint32',
        '5': 'int64',
        '6': 'Message.HistorySyncNotification',
        '7': 'Message.AppStateSyncKeyShare',
        '8': 'Message.AppStateSyncKeyRequest',
        '9': 'Message.InitialSecurityNotificationSettingSync',
        '10': 'Message.AppStateFatalExceptionNotification',
        '11': 'DisappearingMode',
        '14': 'Message',
        '15': 'int64',
        '16': 'Message.PeerDataOperationRequestMessage',
        '17': 'Message.PeerDataOperationRequestResponseMessage',
        '18': 'BotFeedbackMessage',
        '19': 'string',
        '20': 'Message.RequestWelcomeMessageMetadata',
        '21': 'MediaNotifyMessage',
        '22': 'Message.CloudAPIThreadControlNotification',
        '23': 'LIDMigrationMappingSyncMessage',
        '24': 'LimitSharing',
        '25': 'bytes',
        '26': 'AIQueryFanout',
        '27': 'MemberLabel',
        '28': 'AIMediaCollectionMessage',
        '29': 'uint32',
        '30': 'Message.ChatThemeSetting',
        '31': 'AIMetadataOperation',
        '32': 'Message.MarkAsVerifiedAction',
        '33': 'CoexStateSync'
    },
    'Message.QuestionResponseMessage': { '1': 'MessageKey', '2': 'string' },
    'Message.ReactionMessage': { '1': 'MessageKey', '2': 'string', '3': 'string', '4': 'int64' },
    'Message.RequestPaymentMessage': {
        '1': 'string',
        '2': 'uint64',
        '3': 'string',
        '4': 'Message',
        '5': 'int64',
        '6': 'Money',
        '7': 'PaymentBackground'
    },
    'Message.RequestPhoneNumberMessage': { '1': 'ContextInfo' },
    'Message.RequestWelcomeMessageMetadata': {
        '1': { enum: [0, 1] },
        '2': { enum: [0, 1] },
        '3': 'BotAgentMetadata'
    },
    'Message.RootSecretDistributeMessage': { '1': 'string' },
    'Message.ScheduledCallCreationMessage': {
        '1': 'int64',
        '2': { enum: [0, 1, 2] },
        '3': 'string'
    },
    'Message.ScheduledCallEditMessage': { '1': 'MessageKey', '2': { enum: [0, 1] } },
    'Message.SecretEncryptedMessage': {
        '1': 'MessageKey',
        '2': 'bytes',
        '3': 'bytes',
        '4': { enum: [0, 1, 2, 3, 4, 5] },
        '5': 'string'
    },
    'Message.SendPaymentMessage': {
        '2': 'Message',
        '3': 'MessageKey',
        '4': 'PaymentBackground',
        '5': 'string'
    },
    'Message.SenderKeyDistributionMessage': { '1': 'string', '2': 'bytes' },
    'Message.SplitPaymentMessage': {
        '1': 'string',
        '2': 'Money',
        '3': 'string',
        '4': 'string',
        '5': 'Message.SplitPaymentParticipant',
        '6': 'int64',
        '17': 'ContextInfo'
    },
    'Message.SplitPaymentParticipant': { '1': 'string', '2': 'Money', '3': { enum: [0, 1] } },
    'Message.SplitPaymentUpdateMessage': { '1': 'string', '2': 'string' },
    'Message.StatusNotificationMessage': {
        '1': 'MessageKey',
        '2': 'MessageKey',
        '3': { enum: [0, 1, 2, 3] }
    },
    'Message.StatusQuestionAnswerMessage': { '1': 'MessageKey', '2': 'string' },
    'Message.StatusQuotedMessage': {
        '1': { enum: [1] },
        '2': 'string',
        '3': 'bytes',
        '4': 'MessageKey'
    },
    'Message.StatusStickerInteractionMessage': {
        '1': 'MessageKey',
        '2': 'string',
        '3': { enum: [0, 1] }
    },
    'Message.StickerMessage': {
        '1': 'string',
        '2': 'bytes',
        '3': 'bytes',
        '4': 'bytes',
        '5': 'string',
        '6': 'uint32',
        '7': 'uint32',
        '8': 'string',
        '9': 'uint64',
        '10': 'int64',
        '11': 'uint32',
        '12': 'bytes',
        '13': 'bool',
        '16': 'bytes',
        '17': 'ContextInfo',
        '18': 'int64',
        '19': 'bool',
        '20': 'bool',
        '21': 'bool',
        '22': 'string',
        '24': 'int32',
        '25': 'string'
    },
    'Message.StickerPackMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'Message.StickerPackMessage.Sticker',
        '5': 'uint64',
        '6': 'bytes',
        '7': 'bytes',
        '8': 'bytes',
        '9': 'string',
        '10': 'string',
        '11': 'ContextInfo',
        '12': 'string',
        '13': 'int64',
        '14': 'string',
        '15': 'string',
        '16': 'bytes',
        '17': 'bytes',
        '18': 'uint32',
        '19': 'uint32',
        '20': 'string',
        '21': 'uint64',
        '22': { enum: [0, 1, 2] }
    },
    'Message.StickerPackMessage.Sticker': {
        '1': 'string',
        '2': 'bool',
        '3': 'string',
        '4': 'string',
        '5': 'bool',
        '6': 'string',
        '7': 'int32'
    },
    'Message.StickerSyncRMRMessage': { '1': 'string', '2': 'string', '3': 'int64' },
    'Message.TemplateButtonReplyMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'ContextInfo',
        '4': 'uint32',
        '5': 'uint32'
    },
    'Message.TemplateMessage': {
        '1': 'Message.TemplateMessage.FourRowTemplate',
        '2': 'Message.TemplateMessage.HydratedFourRowTemplate',
        '3': 'ContextInfo',
        '4': 'Message.TemplateMessage.HydratedFourRowTemplate',
        '5': 'Message.InteractiveMessage',
        '9': 'string'
    },
    'Message.TemplateMessage.FourRowTemplate': {
        '1': 'Message.DocumentMessage',
        '2': 'Message.HighlyStructuredMessage',
        '3': 'Message.ImageMessage',
        '4': 'Message.VideoMessage',
        '5': 'Message.LocationMessage',
        '6': 'Message.HighlyStructuredMessage',
        '7': 'Message.HighlyStructuredMessage',
        '8': 'TemplateButton'
    },
    'Message.TemplateMessage.HydratedFourRowTemplate': {
        '1': 'Message.DocumentMessage',
        '2': 'string',
        '3': 'Message.ImageMessage',
        '4': 'Message.VideoMessage',
        '5': 'Message.LocationMessage',
        '6': 'string',
        '7': 'string',
        '8': 'HydratedTemplateButton',
        '9': 'string',
        '10': 'bool'
    },
    'Message.URLMetadata': { '1': 'uint32' },
    'Message.VideoEndCard': { '1': 'string', '2': 'string', '3': 'string', '4': 'string' },
    'Message.VideoMessage': {
        '1': 'string',
        '2': 'string',
        '3': 'bytes',
        '4': 'uint64',
        '5': 'uint32',
        '6': 'bytes',
        '7': 'string',
        '8': 'bool',
        '9': 'uint32',
        '10': 'uint32',
        '11': 'bytes',
        '12': 'InteractiveAnnotation',
        '13': 'string',
        '14': 'int64',
        '16': 'bytes',
        '17': 'ContextInfo',
        '18': 'bytes',
        '19': { enum: [0, 1, 2, 3] },
        '20': 'bool',
        '21': 'string',
        '22': 'bytes',
        '23': 'bytes',
        '24': 'string',
        '25': 'InteractiveAnnotation',
        '26': 'string',
        '27': 'ProcessedVideo',
        '28': 'uint32',
        '29': 'uint64',
        '30': 'string',
        '31': { enum: [0, 1] }
    },
    MessageAddOn: {
        '1': { enum: [0, 1, 2, 3, 4] },
        '2': 'Message',
        '3': 'int64',
        '4': 'int64',
        '5': { enum: [0, 1, 2, 3, 4, 5] },
        '6': 'MessageAddOnContextInfo',
        '7': 'MessageKey',
        '8': 'LegacyMessage'
    },
    MessageAddOnContextInfo: { '1': 'uint32', '2': { enum: [1, 2] } },
    MessageAssociation: {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20] },
        '2': 'MessageKey',
        '3': 'int32'
    },
    MessageContextInfo: {
        '1': 'DeviceListMetadata',
        '2': 'int32',
        '3': 'bytes',
        '4': 'bytes',
        '5': 'uint32',
        '6': 'bytes',
        '7': 'BotMetadata',
        '8': 'int32',
        '9': { enum: [1, 2] },
        '10': 'MessageAssociation',
        '11': 'bool',
        '12': 'string',
        '13': 'LimitSharing',
        '14': 'LimitSharing',
        '15': 'ThreadID',
        '16': { enum: [0, 1] },
        '17': 'bytes',
        '18': 'NonE2EEAttestation',
        '19': 'bytes'
    },
    MessageKey: { '1': 'string', '2': 'bool', '3': 'string', '4': 'string' },
    Money: { '1': 'int64', '2': 'uint32', '3': 'string' },
    NonE2EEAttestation: { '1': { enum: [0, 1, 2] } },
    NotificationSettings: {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'bool',
        '5': 'bool',
        '6': 'string'
    },
    PastParticipant: { '1': 'string', '2': { enum: [0, 1] }, '3': 'uint64' },
    PastParticipants: { '1': 'string', '2': 'PastParticipant' },
    PaymentBackground: {
        '1': 'string',
        '2': 'uint64',
        '3': 'uint32',
        '4': 'uint32',
        '5': 'string',
        '6': 'fixed32',
        '7': 'fixed32',
        '8': 'fixed32',
        '9': 'PaymentBackground.MediaData',
        '10': { enum: [0, 1] }
    },
    'PaymentBackground.MediaData': {
        '1': 'bytes',
        '2': 'int64',
        '3': 'bytes',
        '4': 'bytes',
        '5': 'string'
    },
    PaymentInfo: {
        '1': { enum: [0, 1] },
        '2': 'uint64',
        '3': 'string',
        '4': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] },
        '5': 'uint64',
        '6': 'MessageKey',
        '7': 'uint64',
        '8': 'bool',
        '9': 'string',
        '10': {
            enum: [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31
            ]
        },
        '11': 'bool',
        '12': 'Money',
        '13': 'Money'
    },
    PhotoChange: { '1': 'bytes', '2': 'bytes', '3': 'uint32' },
    PinInChat: {
        '1': { enum: [0, 1, 2] },
        '2': 'MessageKey',
        '3': 'int64',
        '4': 'int64',
        '5': 'MessageAddOnContextInfo'
    },
    Point: { '1': 'int32', '2': 'int32', '3': 'double', '4': 'double' },
    PollAdditionalMetadata: { '1': 'bool', '2': 'PollAdditionalMetadata.PollNameHashHistoryEntry' },
    'PollAdditionalMetadata.PollNameHashHistoryEntry': { '1': 'string', '2': 'bytes' },
    PollUpdate: {
        '1': 'MessageKey',
        '2': 'Message.PollVoteMessage',
        '3': 'int64',
        '4': 'int64',
        '5': 'bool',
        '6': 'Message.PollUpdateMessageMetadata'
    },
    PremiumMessageInfo: { '1': 'string' },
    ProcessedVideo: {
        '1': 'string',
        '2': 'bytes',
        '3': 'uint32',
        '4': 'uint32',
        '5': 'uint64',
        '6': 'uint32',
        '7': { enum: [0, 1, 2, 3] },
        '8': 'string'
    },
    QuarantinedMessage: { '1': 'bytes', '2': 'string' },
    Reaction: { '1': 'MessageKey', '2': 'string', '3': 'string', '4': 'int64', '5': 'bool' },
    ReportingTokenInfo: { '1': 'bytes', '2': 'uint64' },
    ScheduledMessageMetadata: { '1': 'string', '2': 'bytes', '3': 'uint64' },
    SessionTransparencyMetadata: { '1': 'string', '2': 'string', '3': { enum: [0, 1] } },
    StatusAttribution: {
        '1': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] },
        '2': 'string',
        '3': 'StatusAttribution.StatusReshare',
        '4': 'StatusAttribution.ExternalShare',
        '5': 'StatusAttribution.Music',
        '6': 'StatusAttribution.GroupStatus',
        '7': 'StatusAttribution.RLAttribution',
        '8': 'StatusAttribution.AiCreatedAttribution'
    },
    'StatusAttribution.AiCreatedAttribution': { '1': { enum: [0, 1] } },
    'StatusAttribution.ExternalShare': {
        '1': 'string',
        '2': { enum: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] },
        '3': 'int32',
        '4': 'string'
    },
    'StatusAttribution.GroupStatus': { '1': 'string' },
    'StatusAttribution.Music': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'string',
        '5': 'string',
        '6': 'bool'
    },
    'StatusAttribution.RLAttribution': { '1': { enum: [0, 1, 2, 3] } },
    'StatusAttribution.StatusReshare': {
        '1': { enum: [0, 1, 2, 3, 4] },
        '2': 'StatusAttribution.StatusReshare.Metadata'
    },
    'StatusAttribution.StatusReshare.Metadata': {
        '1': 'int32',
        '2': 'string',
        '3': 'int32',
        '4': 'bool'
    },
    StatusMentionMessage: { '1': 'Message' },
    StatusPSA: { '44': 'uint64', '45': 'uint64' },
    SyncdIndex: { '1': 'bytes' },
    SyncdValue: { '1': 'bytes' },
    TapLinkAction: { '1': 'string', '2': 'string' },
    TemplateButton: {
        '1': 'TemplateButton.QuickReplyButton',
        '2': 'TemplateButton.URLButton',
        '3': 'TemplateButton.CallButton',
        '4': 'uint32'
    },
    'TemplateButton.CallButton': {
        '1': 'Message.HighlyStructuredMessage',
        '2': 'Message.HighlyStructuredMessage'
    },
    'TemplateButton.QuickReplyButton': { '1': 'Message.HighlyStructuredMessage', '2': 'string' },
    'TemplateButton.URLButton': {
        '1': 'Message.HighlyStructuredMessage',
        '2': 'Message.HighlyStructuredMessage'
    },
    ThreadID: { '1': { enum: [0, 1, 2] }, '2': 'MessageKey' },
    UrlTrackingMap: { '1': 'UrlTrackingMap.UrlTrackingMapElement' },
    'UrlTrackingMap.UrlTrackingMapElement': {
        '1': 'string',
        '2': 'string',
        '3': 'string',
        '4': 'uint32'
    },
    UserPassword: {
        '1': { enum: [0, 1] },
        '2': { enum: [0, 1, 2] },
        '3': 'UserPassword.TransformerArg',
        '4': 'bytes'
    },
    'UserPassword.TransformerArg': { '1': 'string', '2': 'UserPassword.TransformerArg.Value' },
    'UserPassword.TransformerArg.Value': { '1': 'bytes', '2': 'uint32' },
    UserReceipt: {
        '1': 'string',
        '2': 'int64',
        '3': 'int64',
        '4': 'int64',
        '5': 'string',
        '6': 'string'
    },
    WallpaperSettings: { '1': 'string', '2': 'uint32', '3': 'bool' },
    WebMessageInfo: {
        '1': 'MessageKey',
        '2': 'Message',
        '3': 'uint64',
        '4': { enum: [0, 1, 2, 3, 4, 5] },
        '5': 'string',
        '6': 'uint64',
        '16': 'bool',
        '17': 'bool',
        '18': 'bool',
        '19': 'string',
        '20': 'bytes',
        '21': 'bool',
        '22': 'bool',
        '23': 'bool',
        '24': {
            enum: [
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
                23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43,
                44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
                65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85,
                86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104,
                105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121,
                122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138,
                139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155,
                156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172,
                173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189,
                190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206,
                207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
                224, 225, 226, 227, 228, 230
            ]
        },
        '25': 'bool',
        '26': 'string',
        '27': 'uint32',
        '28': 'string',
        '29': 'PaymentInfo',
        '30': 'Message.LiveLocationMessage',
        '31': 'PaymentInfo',
        '32': 'uint64',
        '33': 'uint32',
        '34': 'bool',
        '35': 'bool',
        '36': { enum: [0, 1, 2, 3] },
        '37': 'string',
        '38': 'MediaData',
        '39': 'PhotoChange',
        '40': 'UserReceipt',
        '41': 'Reaction',
        '42': 'MediaData',
        '43': 'bytes',
        '44': 'StatusPSA',
        '45': 'PollUpdate',
        '46': 'PollAdditionalMetadata',
        '47': 'string',
        '48': 'bool',
        '49': 'bytes',
        '50': 'KeepInChat',
        '51': 'string',
        '52': 'uint64',
        '54': 'PinInChat',
        '55': 'PremiumMessageInfo',
        '56': 'bool',
        '57': 'bool',
        '58': 'string',
        '59': 'CommentMetadata',
        '61': 'EventResponse',
        '62': 'ReportingTokenInfo',
        '63': 'uint64',
        '64': 'EventAdditionalMetadata',
        '65': 'bool',
        '66': 'string',
        '67': 'MessageKey',
        '68': 'MessageAddOn',
        '69': 'StatusMentionMessage',
        '70': 'bool',
        '71': 'string',
        '72': 'Citation',
        '73': 'string',
        '74': 'GroupHistoryIndividualMessageInfo',
        '75': 'GroupHistoryBundleInfo',
        '76': 'InteractiveMessageAdditionalMetadata',
        '77': 'QuarantinedMessage',
        '78': 'uint32',
        '79': 'string',
        '80': 'uint64',
        '81': 'ScheduledMessageMetadata',
        '82': 'string',
        '83': 'string'
    }
}

const MAX_DEPTH = 32
const UINT32_MAX = 0xffff_ffffn
const UINT64_MAX = 0xffff_ffff_ffff_ffffn

/** Internal, per-batch validation ports. The enclosing stream charges its own fields. */
export interface HistoryProtocolValidationContext {
    readonly signal: AbortSignal
    readonly maxFieldBytes: number
    readonly chargeField: () => void
}

interface WalkContext {
    readonly validation: HistoryProtocolValidationContext
    readonly output: Uint8Array | null
    outerCount: number
    quotedCount: number
    workBytes: number
    workFields: number
}

interface FieldSpan {
    readonly start: number
    readonly tagEnd: number
    readonly payloadStart: number
    readonly end: number
    readonly number: number
    readonly wire: number
    readonly integer: bigint
}

function reject(category: string): never {
    throw new Error(category)
}

function checkAbort(ctx: WalkContext): void {
    if (ctx.validation.signal.aborted) reject('history_protocol_cancelled')
}

async function checkpoint(ctx: WalkContext, bytes = 0): Promise<void> {
    checkAbort(ctx)
    ctx.workBytes += bytes
    ctx.workFields += 1
    if (ctx.workBytes >= 4_096 || ctx.workFields >= 128) {
        ctx.workBytes = 0
        ctx.workFields = 0
        await yieldImmediate()
        checkAbort(ctx)
    }
}

function varint(bytes: Uint8Array, offset: number, end: number): { value: bigint; end: number } {
    let value = 0n
    for (let index = 0; index < 10; index += 1) {
        if (offset >= end) reject('malformed_history_protocol_varint')
        const byte = bytes[offset++]
        if (index === 9 && byte > 1) reject('malformed_history_protocol_varint')
        value |= BigInt(byte & 0x7f) << BigInt(index * 7)
        if ((byte & 0x80) === 0) return { value, end: offset }
    }
    return reject('malformed_history_protocol_varint')
}

function fieldSpan(bytes: Uint8Array, start: number, end: number, maxBytes: number): FieldSpan {
    const tag = varint(bytes, start, end)
    if (tag.value < 8n || tag.value > UINT32_MAX) reject('malformed_history_protocol_tag')
    const number = Number(tag.value >> 3n)
    const wire = Number(tag.value & 7n)
    let payloadStart = tag.end
    let fieldEnd = tag.end
    let integer = 0n
    if (wire === 0) {
        const scalar = varint(bytes, tag.end, end)
        integer = scalar.value
        fieldEnd = scalar.end
    } else if (wire === 2) {
        const length = varint(bytes, tag.end, end)
        if (length.value > BigInt(maxBytes) || length.value > BigInt(end - length.end)) {
            reject('malformed_or_oversized_history_protocol_length')
        }
        payloadStart = length.end
        fieldEnd = payloadStart + Number(length.value)
    } else if (wire === 1 || wire === 5) {
        fieldEnd += wire === 1 ? 8 : 4
        if (fieldEnd > end) reject('malformed_history_protocol_fixed')
    } else {
        reject('unsupported_history_protocol_wire')
    }
    return { start, tagEnd: tag.end, payloadStart, end: fieldEnd, number, wire, integer }
}

async function scanBytes(ctx: WalkContext, length: number): Promise<void> {
    for (let offset = 0; offset < length; offset += 4_096) {
        await checkpoint(ctx, Math.min(4_096, length - offset))
    }
}

async function validateUtf8(
    bytes: Uint8Array,
    start: number,
    end: number,
    ctx: WalkContext
): Promise<void> {
    let checked = start
    for (let i = start; i < end; ) {
        if (i - checked >= 4_092) {
            await checkpoint(ctx, i - checked)
            checked = i
        }
        const first = bytes[i++]
        if (first <= 0x7f) continue
        let count: number
        let low = 0x80
        let high = 0xbf
        if (first >= 0xc2 && first <= 0xdf) count = 1
        else if (first >= 0xe0 && first <= 0xef) {
            count = 2
            if (first === 0xe0) low = 0xa0
            if (first === 0xed) high = 0x9f
        } else if (first >= 0xf0 && first <= 0xf4) {
            count = 3
            if (first === 0xf0) low = 0x90
            if (first === 0xf4) high = 0x8f
        } else reject('malformed_history_protocol_utf8')
        if (i + count > end || bytes[i] < low || bytes[i] > high)
            reject('malformed_history_protocol_utf8')
        i += 1
        for (let remaining = 1; remaining < count; remaining += 1) {
            if (bytes[i] < 0x80 || bytes[i] > 0xbf) reject('malformed_history_protocol_utf8')
            i += 1
        }
    }
    await checkpoint(ctx, end - checked)
}

function scalarWire(rule: FieldRule): number {
    if (typeof rule !== 'string') return 'enum' in rule ? 0 : 2
    if (rule === 'double' || rule === 'fixed64' || rule === 'sfixed64') return 1
    if (rule === 'float' || rule === 'fixed32' || rule === 'sfixed32') return 5
    if (['uint32', 'uint64', 'int32', 'int64', 'sint32', 'sint64', 'bool'].includes(rule)) return 0
    return 2
}

function validateInteger(rule: FieldRule, value: bigint): void {
    if (typeof rule !== 'string') {
        if (
            !('enum' in rule) ||
            !rule.enum.some((entry) => BigInt(entry) === BigInt.asIntN(64, value))
        ) {
            reject('invalid_history_protocol_enum')
        }
        return
    }
    if ((rule === 'uint32' || rule === 'sint32') && value > UINT32_MAX)
        reject('invalid_history_protocol_uint32')
    if (rule === 'bool' && value !== 0n && value !== 1n) reject('invalid_history_protocol_bool')
    if (rule === 'int32' && value > 0x7fff_ffffn && value < UINT64_MAX - 0x7fff_ffffn) {
        reject('invalid_history_protocol_int32')
    }
}

function unknownCategory(type: string): string {
    if (type === 'HistorySyncMsg' || type === 'WebMessageInfo')
        return 'unknown_or_missing_message_record'
    if (type === 'Message' || type === 'MessageKey') return 'unknown_message_fields'
    return 'unknown_extended_text_fields'
}

async function copySpan(
    ctx: WalkContext,
    source: Uint8Array,
    start: number,
    end: number,
    target: number
): Promise<number> {
    for (let cursor = start; cursor < end; cursor += 4_096) {
        const next = Math.min(end, cursor + 4_096)
        await checkpoint(ctx, next - cursor)
        ctx.output!.set(source.subarray(cursor, next), target)
        target += next - cursor
    }
    return target
}

function encodeLength(output: Uint8Array, value: number, offset: number): number {
    do {
        const next = Math.floor(value / 128)
        output[offset++] = (value % 128) | (next ? 0x80 : 0)
        value = next
    } while (value)
    return offset
}

async function validateGroup(
    bytes: Uint8Array,
    field: FieldSpan,
    previous: Uint8Array | null,
    ctx: WalkContext
): Promise<Uint8Array> {
    const value = bytes.subarray(field.payloadStart, field.end)
    if (value.length === 0) reject('invalid_history_group_address')
    try {
        const address = new TextDecoder('utf-8', { fatal: true }).decode(value)
        const parsed = splitJid(address)
        if (parsed.server !== 'g.us' || !isGroupJid(address))
            reject('invalid_history_group_address')
        parseJidFull(address)
    } catch {
        reject('invalid_history_group_address')
    }
    if (previous) {
        if (previous.length !== value.length) reject('conflicting_history_group_address')
        for (let i = 0; i < value.length; i += 1) {
            if (i % 4_096 === 0) await checkpoint(ctx, Math.min(4_096, value.length - i))
            if (previous[i] !== value[i]) reject('conflicting_history_group_address')
        }
    }
    return value
}

async function walkMessage(
    type: string,
    bytes: Uint8Array,
    start: number,
    end: number,
    ctx: WalkContext,
    depth: number,
    quoted: boolean,
    target = 0
): Promise<{ readonly size: number; readonly changed: boolean }> {
    checkAbort(ctx)
    if (depth > MAX_DEPTH) reject('history_protocol_depth_limit')
    const rules = PINNED_RULES[type]
    if (!rules) reject('unsupported_history_protocol_type')
    let group: Uint8Array | null = null
    let size = 0
    let changed = false
    for (let cursor = start; cursor < end; ) {
        await checkpoint(ctx)
        const field = fieldSpan(bytes, cursor, end, ctx.validation.maxFieldBytes)
        cursor = field.end
        if (!ctx.output) ctx.validation.chargeField()
        const rule = rules[field.number]
        if (!rule) reject(unknownCategory(type))
        const effective = typeof rule !== 'string' && 'packed' in rule ? rule.packed : rule
        const packed = typeof rule !== 'string' && 'packed' in rule && field.wire === 2
        if (field.wire !== scalarWire(effective) && !packed)
            reject('malformed_history_protocol_field_wire')
        if (packed) {
            const expected = scalarWire(effective)
            for (let at = field.payloadStart; at < field.end; ) {
                await checkpoint(ctx)
                if (expected === 0) {
                    const item = varint(bytes, at, field.end)
                    validateInteger(effective, item.value)
                    at = item.end
                } else {
                    at += expected === 1 ? 8 : 4
                    if (at > field.end) reject('malformed_history_protocol_packed')
                }
            }
        } else if (field.wire === 0) validateInteger(effective, field.integer)
        else if (effective === 'string' || effective === 'excludedCanonicalUrl') {
            await validateUtf8(bytes, field.payloadStart, field.end, ctx)
        } else if (effective === 'bytes') await scanBytes(ctx, field.end - field.payloadStart)
        if (type === 'PastParticipants' && field.number === 1)
            group = await validateGroup(bytes, field, group, ctx)
        if (effective === 'excludedCanonicalUrl') {
            changed = true
            if (!ctx.output) {
                if (quoted) ctx.quotedCount += 1
                else ctx.outerCount += 1
            }
            continue
        }
        const child = typeof effective === 'string' ? PINNED_RULES[effective] : null
        if (child) {
            const childQuoted = quoted || (type === 'ContextInfo' && field.number === 3)
            if (ctx.output) {
                const fieldTarget = target
                target = await copySpan(ctx, bytes, field.start, field.tagEnd, target)
                const lengthStart = target
                const oldWidth = field.payloadStart - field.tagEnd
                target += oldWidth
                const childStart = target
                const childResult = await walkMessage(
                    effective as string,
                    bytes,
                    field.payloadStart,
                    field.end,
                    ctx,
                    depth + 1,
                    childQuoted,
                    target
                )
                target = childResult.size
                if (!childResult.changed) {
                    // Preserve even nonminimal framing in unrelated known child fields.
                    target = await copySpan(ctx, bytes, field.start, field.end, fieldTarget)
                    continue
                }
                changed = true
                const prefixEnd = encodeLength(ctx.output, target - childStart, lengthStart)
                const shift = childStart - prefixEnd
                if (shift < 0) reject('history_protocol_projection_invariant')
                if (shift) {
                    for (let from = childStart; from < target; from += 4_096) {
                        const stop = Math.min(target, from + 4_096)
                        await checkpoint(ctx, stop - from)
                        ctx.output.copyWithin(from - shift, from, stop)
                    }
                    target -= shift
                }
            } else {
                const childResult = await walkMessage(
                    effective as string,
                    bytes,
                    field.payloadStart,
                    field.end,
                    ctx,
                    depth + 1,
                    childQuoted
                )
                if (!childResult.changed) {
                    size += field.end - field.start
                    continue
                }
                changed = true
                const childSize = childResult.size
                let width = 1
                for (let value = childSize; value >= 128; value = Math.floor(value / 128))
                    width += 1
                size += field.tagEnd - field.start + width + childSize
            }
        } else if (ctx.output) target = await copySpan(ctx, bytes, field.start, field.end, target)
        else size += field.end - field.start
    }
    if (type === 'PastParticipants' && !group) reject('invalid_history_group_address')
    return { size: ctx.output ? target : size, changed }
}

function walkContext(
    validation: HistoryProtocolValidationContext,
    output: Uint8Array | null = null
): WalkContext {
    return { validation, output, outerCount: 0, quotedCount: 0, workBytes: 0, workFields: 0 }
}

/** Validates only the five accepted root cases; its stream-level occurrence is charged by the caller. */
export async function validateAcceptedHistoryRoot(
    fieldNumber: number,
    wireType: number,
    raw: Uint8Array,
    validation: HistoryProtocolValidationContext
): Promise<void> {
    const ctx = walkContext(validation)
    await checkpoint(ctx)
    const field = fieldSpan(raw, 0, raw.length, validation.maxFieldBytes)
    if (field.number !== fieldNumber || field.wire !== wireType || field.end !== raw.length)
        reject('malformed_history_root_family')
    if (![8, 9, 10, 12, 13].includes(fieldNumber) || field.wire !== (fieldNumber === 10 ? 0 : 2))
        reject('malformed_history_root_family')
    if (fieldNumber === 10) validateInteger('uint32', field.integer)
    else if (fieldNumber === 9) await scanBytes(ctx, field.end - field.payloadStart)
    else
        await walkMessage(
            fieldNumber === 8
                ? 'GlobalSettings'
                : fieldNumber === 12
                  ? 'PastParticipants'
                  : 'CallLogRecord',
            raw,
            field.payloadStart,
            field.end,
            ctx,
            0,
            false
        )
    checkAbort(ctx)
}

/** Validates all physical occurrences before decoding and strips only exact-type legacy URL fields. */
export async function prepareBoundaryHistoryRecord(
    record: Uint8Array,
    validation: HistoryProtocolValidationContext
): Promise<{
    readonly bytes: Uint8Array
    readonly outerCount: number
    readonly quotedCount: number
}> {
    if (record.length > validation.maxFieldBytes) reject('history_protocol_record_limit')
    const ctx = walkContext(validation)
    const measured = await walkMessage('HistorySyncMsg', record, 0, record.length, ctx, 0, false)
    checkAbort(ctx)
    if (ctx.outerCount + ctx.quotedCount === 0)
        return { bytes: record, outerCount: 0, quotedCount: 0 }
    const output = new Uint8Array(record.length)
    const projected = walkContext(validation, output)
    const written = (
        await walkMessage('HistorySyncMsg', record, 0, record.length, projected, 0, false)
    ).size
    if (written !== measured.size || written > record.length)
        reject('history_protocol_projection_invariant')
    checkAbort(ctx)
    return {
        bytes: output.subarray(0, written),
        outerCount: ctx.outerCount,
        quotedCount: ctx.quotedCount
    }
}
