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
    @Test fun cameraDenialOffersManualEntryWithARescanAction() {
        val model = PairingViewModel(ApplicationProvider.getApplicationContext())
        model.cameraUnavailable()
        assertEquals(PairingScreen.MANUAL, model.state.value.screen)
        assertFalse(model.state.value.busy)
        assertTrue(model.state.value.outcome!!.message.contains("API key manual"))
        model.show(PairingScreen.SCANNER)
        assertEquals(PairingScreen.SCANNER, model.state.value.screen)
        assertNull(model.state.value.outcome)
    }
}
