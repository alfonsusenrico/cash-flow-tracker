package com.alfonsusenrico.financialtracker.listener

import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingScreen
import com.alfonsusenrico.financialtracker.listener.ui.pairing.PairingViewModel
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.robolectric.annotation.Config

@RunWith(AndroidJUnit4::class)
@Config(sdk = [34], application = TestApplication::class)
class PairingViewModelTest {
    @Test fun cameraDenialOffersTheTypedCodeWithARescanAction() {
        val model = PairingViewModel(ApplicationProvider.getApplicationContext())
        model.cameraUnavailable()
        assertEquals(PairingScreen.CODE, model.state.value.screen)
        assertFalse(model.state.value.busy)
        assertTrue(model.state.value.outcome!!.message.contains("ketik kodenya"))
        model.show(PairingScreen.SCANNER)
        assertEquals(PairingScreen.SCANNER, model.state.value.screen)
        assertNull(model.state.value.outcome)
    }

    @Test fun firstRunStartsAtTheWelcomeCardAndReplacingStartsAtTheChoice() {
        val setup = PairingViewModel(ApplicationProvider.getApplicationContext())
        setup.start(setup = true, manual = false)
        assertEquals(PairingScreen.WELCOME, setup.state.value.screen)
        assertTrue(setup.state.value.setup)
        val repair = PairingViewModel(ApplicationProvider.getApplicationContext())
        repair.start(setup = false, manual = false)
        assertEquals(PairingScreen.CHOOSE, repair.state.value.screen)
        val manual = PairingViewModel(ApplicationProvider.getApplicationContext())
        manual.start(setup = false, manual = true)
        assertEquals(PairingScreen.MANUAL, manual.state.value.screen)
    }

    @Test fun aTypedCodeIsCheckedOnThePhoneBeforeItIsConfirmed() {
        val model = PairingViewModel(ApplicationProvider.getApplicationContext())
        model.show(PairingScreen.CODE)
        model.typed("7K4-Q9")
        assertEquals(PairingScreen.CODE, model.state.value.screen)
        assertEquals("Kodenya 10 huruf atau angka, ada di bawah QR.", model.state.value.codeError)
        model.typed("7k4q-9mzx-b0")
        assertEquals(PairingScreen.CONFIRM, model.state.value.screen)
        assertNull(model.state.value.codeError)
    }

    @Test fun skippingTheTestNotificationNeedsAFailedNotificationStep() {
        val model = PairingViewModel(ApplicationProvider.getApplicationContext())
        model.show(PairingScreen.SELF_TEST)
        model.skipPost()
        assertEquals(PairingScreen.SELF_TEST, model.state.value.screen)
        assertFalse(model.state.value.postSkipped)
    }
}
