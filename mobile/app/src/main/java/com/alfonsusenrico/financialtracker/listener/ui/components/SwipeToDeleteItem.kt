package com.alfonsusenrico.financialtracker.listener.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.SwipeToDismissBox
import androidx.compose.material3.SwipeToDismissBoxValue
import androidx.compose.material3.Text
import androidx.compose.material3.SwipeToDismissBoxState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.alfonsusenrico.financialtracker.listener.ui.theme.CompanionTheme

const val DELETE_ACTION_LABEL = "Hapus dari aplikasi"

/**
 * Swipe right-to-left past 40 % of the width to delete; a red trash background shows while dragging.
 * The content is the focusable item, so it carries the TalkBack delete action ([DELETE_ACTION_LABEL]).
 */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun SwipeToDeleteItem(
    onDelete: () -> Unit,
    modifier: Modifier = Modifier,
    content: @Composable () -> Unit
) {
    // Not saveable on purpose: a row restored by "Urungkan" gets its old list key back, and a saved
    // dismissed state would delete it again at once.
    val density = LocalDensity.current
    val state = remember(density) {
        SwipeToDismissBoxState(SwipeToDismissBoxValue.Settled, density, positionalThreshold = { width -> width * 0.4f })
    }
    LaunchedEffect(state.currentValue) {
        if (state.currentValue == SwipeToDismissBoxValue.EndToStart) onDelete()
    }
    SwipeToDismissBox(
        state = state,
        enableDismissFromStartToEnd = false,
        modifier = modifier,
        backgroundContent = {
            val dragging = state.dismissDirection == SwipeToDismissBoxValue.EndToStart
            val colors = CompanionTheme.colors
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .clip(RoundedCornerShape(18.dp))
                    .background(if (dragging) colors.danger else Color.Transparent)
                    .padding(horizontal = 20.dp),
                contentAlignment = Alignment.CenterEnd
            ) {
                if (dragging) {
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                        Text("Hapus", color = colors.onDanger, fontSize = 14.sp, fontWeight = FontWeight.SemiBold)
                        Icon(Icons.Outlined.Delete, contentDescription = null, tint = colors.onDanger)
                    }
                }
            }
        }
    ) {
        content()
    }
}
