import React from 'react'
import { Weak } from '../../../../helpers'
import { Translator } from '../../../../translator'
import { PropsUIPageDonation } from '../../../../types/pages'
import { isPropsUIPromptConfirm, isPropsUIPromptConsentForm, isPropsUIPromptFileInput, isPropsUIPromptRadioInput, isPropsUIPromptQuestionnaire } from '../../../../types/prompts'
import { ReactFactoryContext } from '../../factory'
import { Page } from './templates/page'
import { Progress } from '../elements/progress'
import { Title1 } from '../elements/text'
import { Confirm } from '../prompts/confirm'
import { ConsentForm } from '../prompts/consent_form'
import { FileInput } from '../prompts/file_input'
import { Questionnaire } from '../prompts/questionnaire'
import { RadioInput } from '../prompts/radio_input'
import { Footer } from './templates/footer'

type Props = Weak<PropsUIPageDonation> & ReactFactoryContext

export const DonationPage = (props: Props): JSX.Element => {
  const { title } = prepareCopy(props)
  // const { platform, locale, resolve } = props
  const { locale, resolve } = props

  function renderBody (props: Props): JSX.Element {
    const context = { locale: locale, resolve: props.resolve }
    const body = props.body
    if (isPropsUIPromptFileInput(body)) {
      return <FileInput {...body} {...context} />
    }
    if (isPropsUIPromptConfirm(body)) {
      return <Confirm {...body} {...context} />
    }
    if (isPropsUIPromptConsentForm(body)) {
      return <ConsentForm {...body} {...context} />
    }
    if (isPropsUIPromptRadioInput(body)) {
      return <RadioInput {...body} {...context} />
    }
    if (isPropsUIPromptQuestionnaire(body)) {
      return <Questionnaire {...body} {...context} />
    }
    throw new TypeError('Unknown body type')
  }

  function renderFooter (props: Props): JSX.Element | undefined {
    if (props.footer != null) {
      return (
        <Footer
          middle={<Progress percentage={props.footer?.progressPercentage ?? 0} />}
        />
      )
    } else {
      return undefined
    }
  }

  const footer: JSX.Element | undefined = (
    <>
      {renderFooter(props)}
    </>
  )

  const body: JSX.Element = (
    <>
      <Title1 text={title} />
      {renderBody(props)}
    </>
  )

  return <Page body={body} footer={footer} />
}

function prepareCopy ({ header: { title }, locale }: Props): { title: string } {
  return {
    title: Translator.translate(title, locale)
  }
}
